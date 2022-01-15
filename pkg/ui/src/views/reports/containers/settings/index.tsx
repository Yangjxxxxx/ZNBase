  
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { refreshSettings } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import Loading from "src/views/shared/components/loading";
import mapDec from "./decData"
import "./index.styl";


interface SettingsOwnProps {
  settings: CachedDataReducerState<protos.znbase.server.serverpb.SettingsResponse>;
  refreshSettings: typeof refreshSettings;
}

type SettingsProps = SettingsOwnProps;

/**
 * Renders the Cluster Settings Report page.
 */
class Settings extends React.Component<SettingsProps, {}> {
  refresh(props = this.props) {
    props.refreshSettings(new protos.znbase.server.serverpb.SettingsRequest());
  }

  componentWillMount() {
    // Refresh settings query when mounting.
    this.refresh();
  }

  renderTable() {
    if (_.isNil(this.props.settings.data)) {
      return null;
    }

    const { key_values } = this.props.settings.data;
    //console.log(key_values["version"].value);
    key_values["version"].value = "1.0.0";
  
    return (
      <table className="settings-table">
        <thead>
          <tr className="settings-table__row settings-table__row--header">
            <th className="settings-table__cell settings-table__cell--header">设置</th>
            <th className="settings-table__cell settings-table__cell--header">值</th>
            <th className="settings-table__cell settings-table__cell--header">描述</th>
          </tr>
        </thead>
        <tbody>
          {
            _.chain(_.keys(key_values))
              .sort()
              .map(key => (
                <tr key={key} className="settings-table__row">
                  <td className="settings-table__cell">{key}</td>
                  <td className="settings-table__cell">{key_values[key].value}</td>
                  {/* <td className="settings-table__cell">{key_values[key].description}</td> */}
                  <td className="settings-table__cell">{mapDec.get(key)}</td>
                </tr>
              ))
              .value()
          }
        </tbody>
      </table>
    );
  }

  render() {
    return (
      <div className="section">
        <Helmet>
          <title>集群设置 | 调试</title>
        </Helmet>
        <h1>集群设置</h1>
        <Loading
          loading={!this.props.settings.data}
          error={this.props.settings.lastError}
          render={() => (
            <div>
              <p className="settings-note">请注意，出于安全目的，对一些设置进行了修订</p>
              {this.renderTable()}
            </div>
          )}
        />
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    settings: state.cachedData.settings,
  };
}

const actions = {
  refreshSettings,
};

export default connect(mapStateToProps, actions)(Settings);
