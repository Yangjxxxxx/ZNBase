// Copyright 2018  The Cockroach Authors.
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
import PropTypes from "prop-types";
import { Helmet } from "react-helmet";
import { InjectedRouter, RouterState } from "react-router";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import {
  nodeIDAttr, dashboardNameAttr,
} from "src/util/constants";

import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import TimeScaleDropdown from "src/views/cluster/containers/timescale";
import ClusterSummaryBar from "./summaryBar";

import { AdminUIState } from "src/redux/state";
import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { hoverStateSelector, HoverState, hoverOn, hoverOff } from "src/redux/hover";
import { nodesSummarySelector, NodesSummary, LivenessStatus } from "src/redux/nodes";
import Alerts from "src/views/shared/containers/alerts";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

import {
  GraphDashboardProps, storeIDsForNode,
} from "./dashboards/dashboardUtils";

import overviewDashboard from "./dashboards/overview";
import runtimeDashboard from "./dashboards/runtime";
import sqlDashboard from "./dashboards/sql";
import storageDashboard from "./dashboards/storage";
import replicationDashboard from "./dashboards/replication";
import distributedDashboard from "./dashboards/distributed";
import queuesDashboard from "./dashboards/queues";
import requestsDashboard from "./dashboards/requests";
import hardwareDashboard from "./dashboards/hardware";
import changefeedsDashboard from "./dashboards/changefeeds";
import loadDashboard from "./dashboards/load";

interface GraphDashboard {
  label: string;
  component: (props: GraphDashboardProps) => React.ReactElement<any>[];
}

const dashboards: {[key: string]: GraphDashboard} = {
  "overview" : { label: "概览", component: overviewDashboard },
  "hardware": { label: "硬件", component: hardwareDashboard },
  "runtime" : { label: "运行时", component: runtimeDashboard },
  "sql": { label: "SQL", component: sqlDashboard },
  "storage": { label: "存储", component: storageDashboard },
  "replication": { label: "副本", component: replicationDashboard },
  "distributed": { label: "分布", component: distributedDashboard },
  "queues": { label: "队列", component: queuesDashboard },
  "requests": { label: "慢查询", component: requestsDashboard },
  "changefeeds": { label: "CDC", component: changefeedsDashboard },
  "load": { label: "导入导出/备份还原", component: loadDashboard },
};

const defaultDashboard = "概览";

const dashboardDropdownOptions = _.map(dashboards, (dashboard, key) => {
  return {
    value: key,
    label: dashboard.label,
  };
});

// The properties required by a NodeGraphs component.
interface NodeGraphsOwnProps {
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  hoverOn: typeof hoverOn;
  hoverOff: typeof hoverOff;
  nodesQueryValid: boolean;
  livenessQueryValid: boolean;
  nodesSummary: NodesSummary;
  hoverState: HoverState;
}

type NodeGraphsProps = NodeGraphsOwnProps & RouterState;

/**
 * NodeGraphs renders the main content of the cluster graphs page.
 */
class NodeGraphs extends React.Component<NodeGraphsProps, {}> {
  // Magic to add react router to the context.
  // See https://github.com/ReactTraining/react-router/issues/975
  // TODO(mrtracy): Switch this, and the other uses of contextTypes, to use the
  // 'withRouter' HoC after upgrading to react-router 4.x.
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState; };

  /**
   * Selector to compute node dropdown options from the current node summary
   * collection.
   */
  private nodeDropdownOptions = createSelector(
    (summary: NodesSummary) => summary.nodeStatuses,
    (summary: NodesSummary) => summary.nodeDisplayNameByID,
    (summary: NodesSummary) => summary.livenessStatusByNodeID,
    (nodeStatuses, nodeDisplayNameByID, livenessStatusByNodeID): DropdownOption[] => {
      const base = [{value: "", label: "集群"}];
      return base.concat(
        _.chain(nodeStatuses)
          .filter(ns => livenessStatusByNodeID[ns.desc.node_id] !== LivenessStatus.DECOMMISSIONED)
          .map(ns => ({
            value: ns.desc.node_id.toString(),
            label: nodeDisplayNameByID[ns.desc.node_id],
          }))
          .value(),
      );
    },
  );

  refresh(props = this.props) {
    if (!props.nodesQueryValid) {
      props.refreshNodes();
    }
    if (!props.livenessQueryValid) {
      props.refreshLiveness();
    }
  }

  setClusterPath(nodeID: string, dashboardName: string) {
    if (!_.isString(nodeID) || nodeID === "") {
      this.context.router.push(`/metrics/${dashboardName}/cluster`);
    } else {
      this.context.router.push(`/metrics/${dashboardName}/node/${nodeID}`);
    }
  }

  nodeChange = (selected: DropdownOption) => {
    this.setClusterPath(selected.value, this.props.params[dashboardNameAttr]);
  }

  dashChange = (selected: DropdownOption) => {
    this.setClusterPath(this.props.params[nodeIDAttr], selected.value);
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: NodeGraphsProps) {
    this.refresh(props);
  }

  render() {
    const { params, nodesSummary } = this.props;
    const selectedDashboard = params[dashboardNameAttr];
    const dashboard = _.has(dashboards, selectedDashboard)
      ? selectedDashboard
      : defaultDashboard;

    const title = dashboards[dashboard].label + " 面板";
    const selectedNode = params[nodeIDAttr] || "";
    const nodeSources = (selectedNode !== "") ? [selectedNode] : null;

    // When "all" is the selected source, some graphs display a line for every
    // node in the cluster using the nodeIDs collection. However, if a specific
    // node is already selected, these per-node graphs should only display data
    // only for the selected node.
    const nodeIDs = nodeSources ? nodeSources : nodesSummary.nodeIDs;

    // If a single node is selected, we need to restrict the set of stores
    // queried for per-store metrics (only stores that belong to that node will
    // be queried).
    const storeSources = nodeSources ? storeIDsForNode(nodesSummary, nodeSources[0]) : null;

    // tooltipSelection is a string used in tooltips to reference the currently
    // selected nodes. This is a prepositional phrase, currently either "across
    // all nodes" or "on node X".
    const tooltipSelection = (nodeSources && nodeSources.length === 1)
                              ? `在节点 ${nodeSources[0]}`
                              : "所有节点";

    const dashboardProps: GraphDashboardProps = {
      nodeIDs,
      nodesSummary,
      nodeSources,
      storeSources,
      tooltipSelection,
    };

    const forwardParams = {
      hoverOn: this.props.hoverOn,
      hoverOff: this.props.hoverOff,
      hoverState: this.props.hoverState,
    };

    // Generate graphs for the current dashboard, wrapping each one in a
    // MetricsDataProvider with a unique key.
    const graphs = dashboards[dashboard].component(dashboardProps);
    const graphComponents = _.map(graphs, (graph, idx) => {
      const key = `节点.${dashboard}.${idx}`;
      return (
        <div key={key}>
          <MetricsDataProvider id={key}>
            { React.cloneElement(graph, forwardParams) }
          </MetricsDataProvider>
        </div>
      );
    });

    return (
      <div>
        <Helmet>
          <title>{ title }</title>
        </Helmet>
        <section className="section"><h1>{ title }</h1></section>
        <PageConfig>
          <PageConfigItem>
            <Dropdown
              title="图表"
              options={this.nodeDropdownOptions(this.props.nodesSummary)}
              selected={selectedNode}
              onChange={this.nodeChange}
            />
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown
              title="面板"
              options={dashboardDropdownOptions}
              selected={dashboard}
              onChange={this.dashChange}
            />
          </PageConfigItem>
          <PageConfigItem>
            <TimeScaleDropdown />
          </PageConfigItem>
        </PageConfig>
        <section className="section">
          <div className="l-columns">
            <div className="chart-group l-columns__left">
              { graphComponents }
            </div>
            <div className="l-columns__right">
              <Alerts />
              <ClusterSummaryBar nodesSummary={this.props.nodesSummary} nodeSources={nodeSources} />
            </div>
          </div>
        </section>
      </div>
    );
  }
}

export default connect(
  (state: AdminUIState) => {
    return {
      nodesSummary: nodesSummarySelector(state),
      nodesQueryValid: state.cachedData.nodes.valid,
      livenessQueryValid: state.cachedData.nodes.valid,
      hoverState: hoverStateSelector(state),
    };
  },
  {
    refreshNodes,
    refreshLiveness,
    hoverOn,
    hoverOff,
  },
)(NodeGraphs);
