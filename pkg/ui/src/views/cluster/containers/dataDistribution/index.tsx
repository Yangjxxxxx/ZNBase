  
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
import { createSelector } from "reselect";
import { connect } from "react-redux";
import Helmet from "react-helmet";

import Loading from "src/views/shared/components/loading";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import * as docsURL from "src/util/docs";
import { FixLong } from "src/util/fixLong";
import { znbase } from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import {
  refreshDataDistribution,
  refreshNodes,
  refreshLiveness,
  CachedDataReducerState,
} from "src/redux/apiReducers";
import { LocalityTree, selectLocalityTree } from "src/redux/localities";
import ReplicaMatrix, { SchemaObject } from "./replicaMatrix";
import { TreeNode, TreePath } from "./tree";
import "./index.styl";
import {selectLivenessRequestStatus, selectNodeRequestStatus} from "src/redux/nodes";

type DataDistributionResponse = znbase.server.serverpb.DataDistributionResponse;
type NodeDescriptor = znbase.roachpb.INodeDescriptor;
type ZoneConfig$Properties = znbase.server.serverpb.DataDistributionResponse.IZoneConfig;

const ZONE_CONFIG_TEXT = (
  <span>
    区配置
    (<a href={docsURL.configureReplicationZones} target="_blank">看文档</a>)
    控制本数据库如何跨节点分发数据。
  </span>
);

interface DataDistributionProps {
  dataDistribution: CachedDataReducerState<DataDistributionResponse>;
  localityTree: LocalityTree;
  sortedZoneConfigs: ZoneConfig$Properties[];
}

class DataDistribution extends React.Component<DataDistributionProps> {

  renderZoneConfigs() {
    return (
      <div className="zone-config-list">
        <ul>
          {this.props.sortedZoneConfigs.map((zoneConfig) => (
            <li key={zoneConfig.zone_name} className="zone-config">
              <pre className="zone-config__raw-sql">
                {zoneConfig.config_sql}
              </pre>
            </li>
          ))}
        </ul>
      </div>
    );
  }

  getCellValue = (dbPath: TreePath, nodePath: TreePath): number => {
    const [dbName, tableName] = dbPath;
    const nodeID = nodePath[nodePath.length - 1];
    const databaseInfo = this.props.dataDistribution.data.database_info;

    const res = databaseInfo[dbName].table_info[tableName].replica_count_by_node_id[nodeID];
    if (!res) {
      return 0;
    }
    return FixLong(res).toInt();
  }

  render() {
    const nodeTree = nodeTreeFromLocalityTree("集群", this.props.localityTree);

    const databaseInfo = this.props.dataDistribution.data.database_info;
    const dbTree: TreeNode<SchemaObject> = {
      name: "集群",
      data: {
        dbName: null,
        tableName: null,
      },
      children: _.map(databaseInfo, (dbInfo, dbName) => ({
        name: dbName,
        data: {
          dbName,
        },
        children: _.map(dbInfo.table_info, (tableInfo, tableName) => ({
          name: tableName,
          data: {
            dbName,
            tableName,
            droppedAt: tableInfo.dropped_at,
          },
        })),
      })),
    };

    return (
      <div className="data-distribution">
        <div className="data-distribution__zone-config-sidebar">
          <h2>
          区域配置{" "}
            <div className="section-heading__tooltip">
              <ToolTipWrapper text={ZONE_CONFIG_TEXT}>
                <div className="section-heading__tooltip-hover-area">
                  <div className="section-heading__info-icon">i</div>
                </div>
              </ToolTipWrapper>
            </div>
          </h2>
          {this.renderZoneConfigs()}
          <p style={{ maxWidth: 300, paddingTop: 10 }}>
            表现出下降的<span className="table-label--dropped">灰色的</span>.
            它们的副本将根据
            <code>gc.ttlseconds</code> 在它们的区域配置中设置。
          </p>
        </div>
        <div>
          <ReplicaMatrix
            cols={nodeTree}
            rows={dbTree}
            getValue={this.getCellValue}
          />
        </div>
      </div>
    );
  }
}

interface DataDistributionPageProps {
  dataDistribution: CachedDataReducerState<DataDistributionResponse>;
  localityTree: LocalityTree;
  localityTreeErrors: Error[];
  sortedZoneConfigs: ZoneConfig$Properties[];
  refreshDataDistribution: typeof refreshDataDistribution;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

class DataDistributionPage extends React.Component<DataDistributionPageProps> {

  componentDidMount() {
    this.props.refreshDataDistribution();
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps() {
    this.props.refreshDataDistribution();
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  render() {
    return (
      <div>
        <Helmet>
          <title>数据分布</title>
        </Helmet>
        <section className="section">
          <h1>数据分布</h1>
        </section>
        <section className="section">
          <Loading
            loading={!this.props.dataDistribution.data || !this.props.localityTree}
            error={[this.props.dataDistribution.lastError, ...this.props.localityTreeErrors]}
            render={() => (
              <DataDistribution
                localityTree={this.props.localityTree}
                dataDistribution={this.props.dataDistribution}
                sortedZoneConfigs={this.props.sortedZoneConfigs}
              />
            )}
          />
        </section>
      </div>
    );
  }
}

const sortedZoneConfigs = createSelector(
  (state: AdminUIState) => state.cachedData.dataDistribution,
  (dataDistributionState) => {
    if (!dataDistributionState.data) {
      return null;
    }
    return _.sortBy(dataDistributionState.data.zone_configs, (zc) => zc.zone_name);
  },
);

const localityTreeErrors = createSelector(
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  (nodes, liveness) => [nodes.lastError, liveness.lastError],
);

// tslint:disable-next-line:variable-name
const DataDistributionPageConnected = connect(
  (state: AdminUIState) => ({
    dataDistribution: state.cachedData.dataDistribution,
    sortedZoneConfigs: sortedZoneConfigs(state),
    localityTree: selectLocalityTree(state),
    localityTreeErrors: localityTreeErrors(state),
  }),
  {
    refreshDataDistribution,
    refreshNodes,
    refreshLiveness,
  },
)(DataDistributionPage);

export default DataDistributionPageConnected;

// Helpers

function nodeTreeFromLocalityTree(
  rootName: string,
  localityTree: LocalityTree,
): TreeNode<NodeDescriptor> {
  const children: TreeNode<any>[] = [];

  // Add child localities.
  _.forEach(localityTree.localities, (valuesForKey, key) => {
    _.forEach(valuesForKey, (subLocalityTree, value) => {
      children.push(nodeTreeFromLocalityTree(`${key}=${value}`, subLocalityTree));
    });
  });

  // Add child nodes.
  _.forEach(localityTree.nodes, (node) => {
    children.push({
      name: node.desc.node_id.toString(),
      data: node.desc,
    });
  });

  return {
    name: rootName,
    children: children,
  };
}
