  
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
import ReactPaginate from "react-paginate";
import { Link } from "react-router";
import { connect } from "react-redux";

import * as protos from "src/js/protos";

import { AdminUIState } from "src/redux/state";
import { refreshRaft } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

/******************************
 *   RAFT RANGES MAIN COMPONENT
 */

const RANGES_PER_PAGE = 100;

/**
 * RangesMainData are the data properties which should be passed to the RangesMain
 * container.
 */
interface RangesMainData {
  state: CachedDataReducerState<protos.znbase.server.serverpb.RaftDebugResponse>;
}

/**
 * RangesMainActions are the action dispatchers which should be passed to the
 * RangesMain container.
 */
interface RangesMainActions {
  // Call if the ranges statuses are stale and need to be refreshed.
  refreshRaft: typeof refreshRaft;
}

interface RangesMainState {
  showState?: boolean;
  showReplicas?: boolean;
  showPending?: boolean;
  showOnlyErrors?: boolean;
  pageNum?: number;
  offset?: number;
}

/**
 * RangesMainProps is the type of the props object that must be passed to
 * RangesMain component.
 */
type RangesMainProps = RangesMainData & RangesMainActions;

/**
 * Renders the main content of the raft ranges page, which is primarily a data
 * table of all ranges and their replicas.
 */
class RangesMain extends React.Component<RangesMainProps, RangesMainState> {
  state: RangesMainState = {
    showState: true,
    showReplicas: true,
    showPending: true,
    showOnlyErrors: false,
    offset: 0,
  };

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshRaft();
  }

  componentWillReceiveProps(props: RangesMainProps) {
    // Refresh ranges when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    if (!props.state.valid) {
      props.refreshRaft();
    }
  }

  renderPagination(pageCount: number): React.ReactNode {
    return <ReactPaginate previousLabel={"前一个"}
      nextLabel={"下一个"}
      breakLabel={"..."}
      pageCount={pageCount}
      marginPagesDisplayed={2}
      pageRangeDisplayed={5}
      onPageChange={this.handlePageClick.bind(this)}
      containerClassName={"pagination"}
      activeClassName={"active"} />;
  }

  handlePageClick(data: any) {
    const selected = data.selected;
    const offset = Math.ceil(selected * RANGES_PER_PAGE);
    this.setState({ offset });
    window.scroll(0, 0);
  }

  // renderFilterSettings renders the filter settings box.
  renderFilterSettings(): React.ReactNode {
    return <div className="section raft-filters">
      <b>过滤器</b>
      <label>
        <input type="checkbox" checked={this.state.showState}
          onChange={() => this.setState({ showState: !this.state.showState })} />
        状态
      </label>
      <label>
        <input type="checkbox" checked={this.state.showReplicas}
          onChange={() => this.setState({ showReplicas: !this.state.showReplicas })} />
        副本
      </label>
      <label>
        <input type="checkbox" checked={this.state.showPending}
          onChange={() => this.setState({ showPending: !this.state.showPending })} />
        挂起的
      </label>
      <label>
        <input type="checkbox" checked={this.state.showOnlyErrors}
          onChange={() => this.setState({ showOnlyErrors: !this.state.showOnlyErrors })} />
        仅错误的Ranges
      </label>
    </div>;
  }

  render() {
    const statuses = this.props.state.data;
    let content: React.ReactNode = null;
    let errors: string[] = [];

    if (this.props.state.lastError) {
      errors.push(this.props.state.lastError.message);
    }

    if (!this.props.state.data) {
      content = <div className="section">加载中...</div>;
    } else if (statuses) {
      errors = errors.concat(statuses.errors.map(err => err.message));

      // Build list of all nodes for static ordering.
      const nodeIDs = _(statuses.ranges).flatMap((range: protos.znbase.server.serverpb.IRaftRangeStatus) => {
        return range.nodes;
      }).map((node: protos.znbase.server.serverpb.IRaftRangeNode) => {
        return node.node_id;
      }).uniq().sort().value();

      const nodeIDIndex: { [nodeID: number]: number } = {};
      const columns = [<th key={-1}>Range</th>];
      nodeIDs.forEach((id, i) => {
        nodeIDIndex[id] = i + 1;
        columns.push((
          <th key={i}>
            <Link className="debug-link" to={"/nodes/" + id}>节点 {id}</Link>
          </th>
        ));
      });

      // Filter ranges and paginate
      const justRanges = _.values(statuses.ranges);
      const filteredRanges = _.filter(justRanges, (range) => {
        return !this.state.showOnlyErrors || range.errors.length > 0;
      });
      let offset = this.state.offset;
      if (this.state.offset > filteredRanges.length) {
        offset = 0;
      }
      const ranges = filteredRanges.slice(offset, offset + RANGES_PER_PAGE);
      const rows: React.ReactNode[][] = [];
      _.map(ranges, (range, i) => {
        const hasErrors = range.errors.length > 0;
        const rangeErrors = <ul>{_.map(range.errors, (error, j) => {
          return <li key={j}>{error.message}</li>;
        })}</ul>;
        const row = [<td key="row{i}">
          <Link className="debug-link" to={`/reports/range/${range.range_id.toString()}`}>
            r{range.range_id.toString()}
          </Link>
          {
            (hasErrors) ? (
              <span style={{ position: "relative" }}>
                <ToolTipWrapper text={rangeErrors}>
                  <div className="viz-info-icon">
                    <div className="icon-warning" />
                  </div>
                </ToolTipWrapper>
              </span>
            ) : ""
          }
        </td>];
        rows[i] = row;

        // Render each replica into a cell
        range.nodes.forEach((node) => {
          const nodeRange = node.range;
          const replicaLocations = nodeRange.state.state.desc.replicas.map(
            (replica) => "(Node " + replica.node_id.toString() +
              " Store " + replica.store_id.toString() +
              " ReplicaID " + replica.replica_id.toString() + ")",
          );
          const display = (l?: Long): string => {
            if (l) {
              return l.toString();
            }
            return "N/A";
          };
          const index = nodeIDIndex[node.node_id];
          const raftState = nodeRange.raft_state;
          const cell = <td key={index}>
            {(this.state.showState) ? <div>
              State: {raftState.state}&nbsp;
                ReplicaID={display(raftState.replica_id)}&nbsp;
                Term={display(raftState.hard_state.term)}&nbsp;
                Lead={display(raftState.lead)}&nbsp;
            </div> : ""}
            {(this.state.showReplicas) ? <div>
              <div>Replica On: {replicaLocations.join(", ")}</div>
              <div>Next Replica ID: {nodeRange.state.state.desc.next_replica_id}</div>
            </div> : ""}
            {(this.state.showPending) ? <div>Pending Command Count: {(nodeRange.state.num_pending || 0).toString()}</div> : ""}
          </td>;
          row[index] = cell;
        });

        // Fill empty spaces in table with td elements.
        for (let j = 1; j <= nodeIDs.length; j++) {
          if (!row[j]) {
            row[j] = <td key={j}></td>;
          }
        }
      });

      // Build the final display table
      if (columns.length > 1) {
        content = <div>
          {this.renderFilterSettings()}
          <table>
            <thead><tr>{columns}</tr></thead>
            <tbody>
              {_.values(rows).map((row: React.ReactNode[], i: number) => {
                return <tr key={i}>{row}</tr>;
              })}
            </tbody>
          </table>
          <div className="section">
            {this.renderPagination(Math.ceil(filteredRanges.length / RANGES_PER_PAGE))}
          </div>
        </div>;
      }
    }
    return <div className="section table">
      {this.props.children}
      <div className="stats-table">
        {this.renderErrors(errors)}
        {content}
      </div>
    </div>;
  }

  renderErrors(errors: string[]) {
    if (!errors || errors.length === 0) {
      return;
    }
    return <div className="section">
      {errors.map((err: string, i: number) => {
        return <div key={i}>Error: {err}</div>;
      })}
    </div>;
  }
}

/******************************
 *         SELECTORS
 */

// Base selectors to extract data from redux state.
const selectRaftState = (state: AdminUIState): CachedDataReducerState<protos.znbase.server.serverpb.RaftDebugResponse> => state.cachedData.raft;

// Connect the RangesMain class with our redux store.
const rangesMainConnected = connect(
  (state: AdminUIState) => {
    return {
      state: selectRaftState(state),
    };
  },
  {
    refreshRaft: refreshRaft,
  },
)(RangesMain);

export { rangesMainConnected as default };
