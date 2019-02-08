/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.sql;

import co.cask.cdap.api.dataset.lib.AbstractCloseableIterator;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.spi.data.InvalidFieldException;
import co.cask.cdap.spi.data.StructuredRow;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.table.StructuredTableSchema;
import co.cask.cdap.spi.data.table.field.Field;
import co.cask.cdap.spi.data.table.field.FieldType;
import co.cask.cdap.spi.data.table.field.FieldValidator;
import co.cask.cdap.spi.data.table.field.Fields;
import co.cask.cdap.spi.data.table.field.Range;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Sql structured table implementation.
 */
public class PostgresSqlStructuredTable implements StructuredTable {
  private static final Logger LOG = LoggerFactory.getLogger(PostgresSqlStructuredTable.class);

  private final Connection connection;
  private final StructuredTableSchema tableSchema;
  private final FieldValidator fieldValidator;

  public PostgresSqlStructuredTable(Connection connection, StructuredTableSchema tableSchema) {
    this.connection = connection;
    this.tableSchema = tableSchema;
    this.fieldValidator = new FieldValidator(tableSchema);
  }

  @Override
  public void upsert(Collection<Field<?>> fields) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Write fields {}", tableSchema.getTableId(), fields);
    Set<String> fieldNames = fields.stream().map(Field::getName).collect(Collectors.toSet());
    if (!fieldNames.containsAll(tableSchema.getPrimaryKeys())) {
      throw new InvalidFieldException(tableSchema.getTableId(), fields,
                                      String.format("Given fields %s do not contain all the " +
                                                      "primary keys %s", fieldNames, tableSchema.getPrimaryKeys()));
    }
    upsertInternal(fields);
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys) throws InvalidFieldException, IOException {
    return readRow(keys, null);
  }

  @Override
  public Optional<StructuredRow> read(Collection<Field<?>> keys,
                                      Collection<String> columns) throws InvalidFieldException, IOException {
    if (columns == null || columns.isEmpty()) {
      throw new IllegalArgumentException("No columns are specified to read");
    }

    // always have the primary key fields included in the columns
    Set<String> columnFields = new HashSet<>(columns);
    columnFields.addAll(keys.stream().map(Field::getName).collect(Collectors.toSet()));
    return readRow(keys, columnFields);
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Range keyRange, int limit) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Scan range {} with limit {}", tableSchema.getTableId(), keyRange, limit);
    fieldValidator.validatePrimaryKeys(keyRange.getBegin(), true);
    fieldValidator.validatePrimaryKeys(keyRange.getEnd(), true);
    String scanQuery = getScanQuery(keyRange, limit);

    // We don't close the statement here because once it is closed, the result set is also closed.
    try {
      PreparedStatement statement = connection.prepareStatement(scanQuery);
      int index = 1;
      if (keyRange.getBegin() != null) {
        for (Field<?> key : keyRange.getBegin()) {
          setField(statement, key, index);
          index++;
        }
      }
      if (keyRange.getEnd() != null) {
        for (Field<?> key : keyRange.getEnd()) {
          setField(statement, key, index);
          index++;
        }
      }
      LOG.trace("SQL statement: {}", statement);
      ResultSet resultSet = statement.executeQuery();
      return new ResultSetIterator(statement, resultSet, tableSchema);
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to scan from table %s with range %s",
                                          tableSchema.getTableId().getName(), keyRange), e);
    }
  }

  @Override
  public CloseableIterator<StructuredRow> scan(Field<?> index) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Scan index {}", tableSchema.getTableId(), index);
    fieldValidator.validateField(index);
    if (!tableSchema.isIndexColumn(index.getName())) {
      throw new InvalidFieldException(tableSchema.getTableId(), index.getName(), "is not an indexed column");
    }

    String sql = getReadQuery(Collections.singleton(index), null, false);
    // We don't close the statement here because once it is closed, the result set is also closed.
    try {
      PreparedStatement statement = connection.prepareStatement(sql);
      setField(statement, index, 1);
      LOG.trace("SQL statement: {}", statement);
      ResultSet resultSet = statement.executeQuery();
      return new ResultSetIterator(statement, resultSet, tableSchema);
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to scan from table %s with index %s",
                                          tableSchema.getTableId().getName(), index), e);
    }
  }

  @Override
  public boolean compareAndSwap(Collection<Field<?>> keys, Field<?> oldValue, Field<?> newValue)
    throws InvalidFieldException, IOException {
    LOG.trace("Table {}: CompareAndSwap with keys {}, oldValue {}, newValue {}", tableSchema.getTableId(), keys,
              oldValue, newValue);
    fieldValidator.validatePrimaryKeys(keys, false);
    fieldValidator.validateField(oldValue);
    if (oldValue.getFieldType() != newValue.getFieldType()) {
      throw new IllegalArgumentException(
        String.format("Field types of oldValue (%s) and newValue (%s) are not the same",
                      oldValue.getFieldType(), newValue.getFieldType()));
    }
    if (!oldValue.getName().equals(newValue.getName())) {
      throw new IllegalArgumentException(
        String.format("Trying to compare and swap different fields. Old Value = %s, New Value = %s",
                      oldValue, newValue));
    }
    if (tableSchema.isPrimaryKeyColumn(oldValue.getName())) {
      throw new IllegalArgumentException("Cannot use compare and swap on a primary key field");
    }

    // First compare
    String readQuery = getReadQuery(keys, Collections.singleton(oldValue.getName()), true);
    try (PreparedStatement statement = connection.prepareStatement(readQuery)) {
      int index = 1;
      for (Field<?> key : keys) {
        setField(statement, key, index);
        index++;
      }
      LOG.trace("SQL statement: {}", statement);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          // Compare the value in the DB to oldValue
          Object colValue = resultSet.getObject(1);
          Field<?> dbValue = createField(oldValue.getName(), oldValue.getFieldType(), colValue);
          if (!oldValue.equals(dbValue)) {
            return false;
          }
        } else {
          // There is no data for the field in the DB, hence oldValue should be null to continue
          if (oldValue.getValue() != null) {
            return false;
          }
        }
      }
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to read from table %s with keys %s",
                                          tableSchema.getTableId().getName(), keys), e);
    }

    // Then write
    Collection<Field<?>> fields = new HashSet<>(keys);
    fields.add(newValue);
    upsertInternal(fields);

    return true;
  }

  @Override
  public void increment(Collection<Field<?>> keys, String column, long amount)
    throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Increment with keys {}, column {}, amount {}", tableSchema.getTableId(), keys, column, amount);
    FieldType.Type colType = tableSchema.getType(column);
    if (colType == null) {
      throw new InvalidFieldException(tableSchema.getTableId(), column);
    } else if (colType != FieldType.Type.LONG) {
      throw new IllegalArgumentException(
        String.format("Trying to increment a column of type %s. Only %s column type can be incremented",
                      colType, FieldType.Type.LONG));
    }
    if (tableSchema.isPrimaryKeyColumn(column)) {
      throw new IllegalArgumentException("Cannot use increment on a primary key field");
    }
    fieldValidator.validatePrimaryKeys(keys, false);

    List<Field<?>> fieldsWithValue = new ArrayList<>(keys);
    // If the row does not exist, insert it with long field = amount
    fieldsWithValue.add(Fields.longField(column, amount));
    String sql = getWriteSqlQuery(fieldsWithValue, column);
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      int index = 1;
      for (Field<?> key : fieldsWithValue) {
        setField(statement, key, index);
        index++;
      }
      // populate increment amount
      statement.setLong(index, amount);
      LOG.trace("SQL statement: {}", statement);
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to increment column %s of table %s with increment value %d",
                                          column, tableSchema.getTableId().getName(), amount), e);
    }
  }

  @Override
  public void delete(Collection<Field<?>> keys) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Delete with keys {}", tableSchema.getTableId(), keys);
    fieldValidator.validatePrimaryKeys(keys, false);
    String sqlQuery = getDeleteQuery(keys);
    try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
      int index = 1;
      for (Field<?> key : keys) {
        setField(statement, key, index);
        index++;
      }
      LOG.trace("SQL statement: {}", statement);
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to delete the row from table %s with fields %s",
                                          tableSchema.getTableId().getName(), keys), e);
    }
  }

  @Override
  public void deleteAll(Range keyRange) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: DeleteAll with range {}", tableSchema.getTableId(), keyRange);
    fieldValidator.validatePrimaryKeys(keyRange.getBegin(), true);
    fieldValidator.validatePrimaryKeys(keyRange.getEnd(), true);
    String sql = getDeleteAllStatement(keyRange);
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      int index = 1;
      if (keyRange.getBegin() != null) {
        for (Field<?> key : keyRange.getBegin()) {
          setField(statement, key, index);
          index++;
        }
      }
      if (keyRange.getEnd() != null) {
        for (Field<?> key : keyRange.getEnd()) {
          setField(statement, key, index);
          index++;
        }
      }
      LOG.trace("SQL statement: {}", statement);
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to delete the rows from table %s with range %s",
                                          tableSchema.getTableId().getName(), keyRange), e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      connection.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close the SQL connection", e);
    }
  }

  private void upsertInternal(Collection<Field<?>> fields) throws IOException {
    String sqlQuery = getWriteSqlQuery(fields, null);
    try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
      int index = 1;
      for (Field<?> field : fields) {
        setField(statement, field, index);
        index++;
      }
      LOG.trace("SQL statement: {}", statement);
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to write to table %s with fields %s",
                                          tableSchema.getTableId().getName(), fields), e);
    }
  }

  /**
   * Read a row from the table. Null columns mean read from all columns.
   *
   * @param keys key of the row
   * @param columns columns to read, null means read from all
   * @return an optional containing the row or empty optional if the row does not exist
   */
  private Optional<StructuredRow> readRow(
    Collection<Field<?>> keys, @Nullable Collection<String> columns) throws InvalidFieldException, IOException {
    LOG.trace("Table {}: Read with keys {} and columns {}", tableSchema.getTableId(), keys, columns);
    fieldValidator.validatePrimaryKeys(keys, false);
    String readQuery = getReadQuery(keys, columns, false);
    try (PreparedStatement statement = connection.prepareStatement(readQuery);) {
      int index = 1;
      for (Field<?> key : keys) {
        setField(statement, key, index);
        index++;
      }
      LOG.trace("SQL statement: {}", statement);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          return Optional.empty();
        }

        ResultSetMetaData metaData = resultSet.getMetaData();
        int numCols = metaData.getColumnCount();
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= numCols; i++) {
          row.put(metaData.getColumnName(i), resultSet.getObject(i));
        }
        return Optional.of(new SqlStructuredRow(tableSchema, row));
      }
    } catch (SQLException e) {
      throw new IOException(String.format("Failed to read from table %s with keys %s",
                                          tableSchema.getTableId().getName(), keys), e);
    }
  }

  private void setField(PreparedStatement statement, Field field,
                        int parameterIndex) throws SQLException, InvalidFieldException {
    fieldValidator.validateField(field);
    Object value = field.getValue();
    FieldType.Type type = tableSchema.getType(field.getName());
    if (type == null) {
      throw new InvalidFieldException(tableSchema.getTableId(), field.getName());
    }

    switch (type) {
      case INTEGER:
        if (value == null) {
          statement.setNull(parameterIndex, Types.INTEGER);
        } else {
          statement.setInt(parameterIndex, (int) value);
        }
        break;
      case LONG:
        if (value == null) {
          statement.setNull(parameterIndex, Types.BIGINT);
        } else {
          statement.setLong(parameterIndex, (long) value);
        }
        break;
      case FLOAT:
        if (value == null) {
          statement.setNull(parameterIndex, Types.FLOAT);
        } else {
          statement.setFloat(parameterIndex, (float) value);
        }
        break;
      case DOUBLE:
        if (value == null) {
          statement.setNull(parameterIndex, Types.DOUBLE);
        } else {
          statement.setDouble(parameterIndex, (double) value);
        }
        break;
      case STRING:
        if (value == null) {
          statement.setNull(parameterIndex, Types.VARCHAR);
        } else {
          statement.setString(parameterIndex, (String) value);
        }
        break;
      case BYTES:
        if (value == null) {
          statement.setNull(parameterIndex, Types.LONGVARBINARY);
        } else {
          statement.setBytes(parameterIndex, (byte[]) value);
        }
        break;
      default:
        // this should not happen since we validate the field before setting
        throw new InvalidFieldException(tableSchema.getTableId(), field.getName());
    }
  }

  /**
   * Get the write sql query for PreparedStatement for the fields given. For example, if "simpletable" has 5 columns,
   * (key1, key2, col1, col2, col3), this write query will generate the following query:
   * INSERT INTO simpletable (key1,key2,col1,col2,col3) VALUES (?,?,?,?,?) ON CONFLICT (key1,key2)
   * DO UPDATE SET col1=EXCLUDED.col1,col2=EXCLUDED.col2,col3=EXCLUDED.col3;
   *
   * @param fields fields to write
   * @param incrementField the field to increment if conflict. If null, then do not increment
   * @return the sql query
   */
  private String getWriteSqlQuery(Collection<Field<?>> fields, @Nullable String incrementField) {
    StringJoiner insertPart = new StringJoiner(",",
                                               "INSERT INTO " + tableSchema.getTableId().getName() + " (",
                                               ") ");
    StringJoiner valuePart = new StringJoiner(",", "VALUES (", ") ");
    StringJoiner conflictPart = new StringJoiner(",", "ON CONFLICT (", ") ");
    StringJoiner updatePart = new StringJoiner(",",  "DO UPDATE SET ", ";");

    for (Field<?> field : fields) {
      insertPart.add(field.getName());
      valuePart.add("?");
      if (tableSchema.isPrimaryKeyColumn(field.getName())) {
        conflictPart.add(field.getName());
      } else if (incrementField != null && field.getName().equals(incrementField)) {
        updatePart.add(field.getName() + " = " + tableSchema.getTableId().getName() + "." + field.getName() + " + ?");
      } else {
        updatePart.add(field.getName() + "=EXCLUDED." + field.getName());
      }
    }
    return insertPart.toString() + valuePart.toString() + conflictPart.toString() + updatePart.toString();
  }

  private String getReadQuery(Collection<Field<?>> keys, Collection<String> columns, boolean forUpdate) {
    StringBuilder queryString =
      new StringBuilder("SELECT ")
        .append(columns == null ? "*" : Joiner.on(",").join(columns))
        .append(" FROM ")
        .append(tableSchema.getTableId().getName())
        .append(" WHERE ").append(getEqualsClause(keys))
        .append(getOrderByClause(tableSchema.getPrimaryKeys()))
        .append(forUpdate ? " FOR UPDATE " : "")
        .append(";");
    return queryString.toString();
  }

  /**
   * Get the scan query for the range given. For example, if the range provides key1, key2 as the begin and end to
   * scan, both rows are inclusive, it will generate the following query:
   * SELECT * FROM simpletable WHERE (key1,key2)>=(?,?) AND (key1,key2)<=(?,?) LIMIT 10;
   *
   * @param range the range to scan.
   * @param limit limit number of row
   * @return the scan query
   */
  private String getScanQuery(Range range, int limit) {
    StringBuilder queryString = new StringBuilder("SELECT * FROM ").append(tableSchema.getTableId().getName());
    if (!range.getBegin().isEmpty() || !range.getEnd().isEmpty()) {
      queryString.append(" WHERE ");
      appendRange(queryString, range);
    }

    queryString.append(getOrderByClause(tableSchema.getPrimaryKeys()));
    queryString.append(" LIMIT ").append(limit).append(";");
    return queryString.toString();
  }

  private void appendRange(StringBuilder statement, Range range) {
    appendScanBound(statement, range.getBegin(), range.getBeginBound().equals(Range.Bound.INCLUSIVE) ? ">=" : ">");
    if (!range.getBegin().isEmpty() && !range.getEnd().isEmpty()) {
      statement.append(" AND ");
    }
    appendScanBound(statement, range.getEnd(), range.getEndBound().equals(Range.Bound.INCLUSIVE) ? "<=" : "<");
  }

  private void appendScanBound(StringBuilder sb,
                               Collection<Field<?>> keys, String comparator) {
    if (keys.isEmpty()) {
      return;
    }

    StringJoiner keyJoiner = new StringJoiner(",", "(", ")");
    StringJoiner valueJoiner = new StringJoiner(",", "(", ")");
    for (Field<?> field : keys) {
      keyJoiner.add(field.getName());
      valueJoiner.add("?");
    }

    sb.append(keyJoiner.toString())
      .append(comparator)
      .append(valueJoiner.toString());
  }

  private String getDeleteQuery(Collection<Field<?>> keys) {
    return String.format("DELETE FROM %s WHERE %s;", tableSchema.getTableId().getName(), getEqualsClause(keys));
  }

  private String getDeleteAllStatement(Range range) {
    StringBuilder statement = new StringBuilder("DELETE FROM ").append(tableSchema.getTableId().getName());

    if (!range.getBegin().isEmpty() || !range.getEnd().isEmpty()) {
      statement.append(" WHERE ");
      appendRange(statement, range);
    }
    return statement.toString();
  }

  private String getEqualsClause(Collection<Field<?>> keys) {
    StringJoiner joiner = new StringJoiner(" AND ");
    for (Field<?> key : keys) {
      joiner.add(key.getName() + "=?");
    }
    return joiner.toString();
  }

  private String getOrderByClause(List<String> keys) {
    StringJoiner joiner = new StringJoiner(", ", " ORDER BY ", "");
    for (String key : keys) {
      joiner.add(key);
    }
    return joiner.toString();
  }

  private Field<?> createField(String name, FieldType.Type type, Object value) {
    switch (type) {
      case BYTES:
        return Fields.bytesField(name, (byte[]) value);
      case LONG:
        return Fields.longField(name, (Long) value);
      case INTEGER:
        return Fields.intField(name, (Integer) value);
      case DOUBLE:
        return Fields.doubleField(name, (Double) value);
      case FLOAT:
        return Fields.floatField(name, (Float) value);
      case STRING:
        return Fields.stringField(name, (String) value);
        default:
          throw new IllegalStateException("Unknown field type " + type);
    }
  }

  private static final class ResultSetIterator extends AbstractCloseableIterator<StructuredRow> {
    private final Statement statement;
    private final ResultSet resultSet;
    private final Set<String> columnNames;
    private final StructuredTableSchema schema;


    ResultSetIterator(Statement statement, ResultSet resultSet, StructuredTableSchema schema) throws SQLException {
      this.statement = statement;
      this.resultSet = resultSet;
      this.columnNames = createColNames(resultSet.getMetaData());
      this.schema = schema;
    }

    @Override
    protected StructuredRow computeNext() {
      try {
        if (!resultSet.next()) {
          return endOfData();
        }

        Map<String, Object> row = new HashMap<>();
        for (String colName : columnNames) {
          row.put(colName, resultSet.getObject(colName));
        }
        return new SqlStructuredRow(schema, row);
      } catch (SQLException e) {
        throw new RuntimeException("Failed to get the next value from the sql result set", e);
      }
    }

    @Override
    public void close() {
      try {
        statement.close();
        resultSet.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close the result set", e);
      }
    }

    private static Set<String> createColNames(ResultSetMetaData metaData) throws SQLException {
      Set<String> columns = new HashSet<>();
      int numCols = metaData.getColumnCount();
      for (int i = 1; i <= numCols; i++) {
        columns.add(metaData.getColumnName(i));
      }
      return columns;
    }
  }
}
