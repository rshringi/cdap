/*
 * Copyright © 2018 Cask Data, Inc.
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

import * as React from 'react';
import List from '@material-ui/core/List';
import Drawer from '@material-ui/core/Drawer';
import NamespaceDropdown from 'components/NamespaceDropdown';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
const colorVariables = require('styles/variables.scss');
import { Link } from 'react-router-dom';
import { withContext, INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import DrawerFeatureLink from 'components/AppHeader/AppDrawer/DrawerFeatureLink';
import { Theme } from 'services/ThemeHelper';
import ee from 'event-emitter';

const DRAWER_WIDTH = '240px';
export const appDrawerListItemTextStyles = {
  fontWeight: 400,
  fontSize: '14px',
};
export const appDrawerListItemStyles = {
  '&:hover': {
    backgroundColor: colorVariables.grey06,
    color: colorVariables.grey02,
  },
};
const styles = (theme) => {
  return {
    drawer: {
      zIndex: theme.zIndex.drawer,
      width: DRAWER_WIDTH,
    },
    drawerPaper: {
      width: DRAWER_WIDTH,
      backgroundColor: colorVariables.grey08,
    },
    listItemText: appDrawerListItemTextStyles,
    toolbar: {
      minHeight: '48px',
    },
    mainMenu: {
      borderTop: `1px solid ${colorVariables.grey06}`,
    },
    namespaceAdminMenu: {
      // WUT TS?
      position: 'absolute' as 'absolute',
      bottom: '0px',
      width: '100%',
      borderTop: `1px solid ${colorVariables.grey06}`,
    },
  };
};

interface IAppDrawerProps extends WithStyles<typeof styles> {
  open: boolean;
  onClose: () => void;
  componentDidNavigate: () => void;
  context: INamespaceLinkContext;
}

class AppDrawer extends React.PureComponent<IAppDrawerProps> {
  private eventEmitter = ee(ee);
  public constructor(props) {
    super(props);
    this.eventEmitter.on('NUX-TOUR-START', this.toggleNuxTourProgressFlag);
    this.eventEmitter.on('NUX-TOUR-END', this.toggleNuxTourProgressFlag);
  }
  public state = {
    onNamespacePreferenceEdit: false,
    nuxTourInProgress: false,
  };
  public toggleNuxTourProgressFlag = () => {
    this.setState({
      nuxTourInProgress: !this.state.nuxTourInProgress,
    });
  };
  public toggleonNamespacePreferenceEdit = () => {
    this.setState({ onNamespacePreferenceEdit: !this.state.onNamespacePreferenceEdit });
  };
  public render() {
    const { classes, open, onClose, componentDidNavigate = () => null } = this.props;
    const { isNativeLink, namespace } = this.props.context;
    const nsurl = `ns/${namespace}`;
    return (
      <Drawer
        open={this.state.nuxTourInProgress || open}
        onClose={onClose}
        className={classes.drawer}
        disableEnforceFocus={true}
        disableEscapeKeyDown={this.state.onNamespacePreferenceEdit}
        ModalProps={{
          keepMounted: true,
        }}
        classes={{
          paper: classes.drawerPaper,
        }}
        data-cy="navbar-drawer"
      >
        <div className={classes.toolbar} />
        <NamespaceDropdown
          onNamespaceCreate={onClose}
          onNamespacePreferenceEdit={this.toggleonNamespacePreferenceEdit}
          onNamespaceChange={onClose}
          tag={isNativeLink ? 'a' : Link}
        />
        <List component="nav" dense={true} className={classes.mainMenu}>
          <DrawerFeatureLink
            featureName={Theme.featureNames.controlCenter}
            featureSVGIconName="icon-control_center"
            featureFlag={true}
            featureUrl={`/${nsurl}`}
            componentDidNavigate={componentDidNavigate}
            data-cy="navbar-control-center-link"
            id="navbar-control-center"
            isActive={
              location.pathname === `/cdap/${nsurl}` ||
              location.pathname.startsWith(`/cdap/${nsurl}/dataset`) ||
              location.pathname.startsWith(`/cdap/${nsurl}/apps`)
            }
          />
          <DrawerFeatureLink
            featureName={Theme.featureNames.pipelines}
            featureSVGIconName="icon-pipeline_hollow"
            subMenu={[
              {
                featureName: Theme.featureNames.pipelinesList,
                featureFlag: Theme.showPipelines,
                featureUrl: `/${nsurl}/pipelines`,
                id: 'navbar-pipelines',
                componentDidNavigate,
                featureSVGIconName: 'icon-list',
                'data-cy': 'navbar-pipelines-link',
              },
              {
                featureName: Theme.featureNames.pipelineStudio,
                featureFlag: Theme.showPipelineStudio,
                featureUrl: `/pipelines/${nsurl}/studio`,
                featureSVGIconName: 'icon-pipeline_filled',
                componentDidNavigate,
                isAngular: true,
                'data-cy': 'navbar-pipeline-studio-link',
              },
            ]}
          />
          <DrawerFeatureLink
            featureName={Theme.featureNames.dataPrep}
            featureFlag={Theme.showDataPrep}
            featureSVGIconName="icon-transform"
            featureUrl={`/${nsurl}/dataprep`}
            componentDidNavigate={componentDidNavigate}
            data-cy="navbar-dataprep-link"
            id="navbar-preparation"
            isActive={
              location.pathname.startsWith(`/cdap/${nsurl}/dataprep`) ||
              location.pathname.startsWith(`/cdap/${nsurl}/connections`)
            }
          />
          <DrawerFeatureLink
            featureUrl={`/${nsurl}/experiments`}
            featureSVGIconName="icon-analytics"
            featureFlag={Theme.showAnalytics}
            featureName={Theme.featureNames.analytics}
            componentDidNavigate={componentDidNavigate}
            data-cy="navbar-experiments-link"
          />
          <DrawerFeatureLink
            featureUrl={`/${nsurl}/rulesengine`}
            featureSVGIconName="icon-rules"
            featureFlag={Theme.showRulesEngine}
            featureName={Theme.featureNames.rulesEngine}
            componentDidNavigate={componentDidNavigate}
            data-cy="navbar-rulesengine-link"
          />
          <DrawerFeatureLink
            featureUrl={`/metadata/${nsurl}`}
            featureSVGIconName="icon-metadata"
            featureFlag={Theme.showMetadata}
            featureName={Theme.featureNames.metadata}
            isAngular={true}
            id="navbar-metadata"
            data-cy="navbar-metadata-link"
          />
        </List>
        <List component="nav" dense={true} className={classes.namespaceAdminMenu}>
          <DrawerFeatureLink
            featureUrl="/administration/configuration"
            featureName={Theme.featureNames.projectAdmin}
            featureFlag={true}
            componentDidNavigate={componentDidNavigate}
            data-cy="navbar-project-admin-link"
          />
        </List>
      </Drawer>
    );
  }
}

export default withStyles(styles)(withContext(AppDrawer));
