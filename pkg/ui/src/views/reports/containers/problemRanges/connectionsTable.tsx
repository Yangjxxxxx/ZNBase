  
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
import classNames from "classnames";
import React from "react";
import { Link } from "react-router";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";

interface ConnectionTableColumn {
  title: string;
  extract: (
    p: protos.znbase.server.serverpb.ProblemRangesResponse.INodeProblems,
    id?: number,
  ) => React.ReactNode;
}

interface ConnectionsTableProps {
  problemRanges: CachedDataReducerState<protos.znbase.server.serverpb.ProblemRangesResponse>;
}

const connectionTableColumns: ConnectionTableColumn[] = [
  {
    title: "节点",
    extract: (_problem, id) => (
      <Link className="debug-link" to={`/reports/problemranges/${id}`}>
        n{id}
      </Link>
    ),
  },
  { title: "不可用的", extract: (problem) => problem.unavailable_range_ids.length },
  { title: "非 Raft Leader", extract: (problem) => problem.no_raft_leader_range_ids.length },
  { title: "无效的 Lease", extract: (problem) => problem.no_lease_range_ids.length },
  {
    title: "Raft Leader 但不是 Lease Holder",
    extract: (problem) => problem.raft_leader_not_lease_holder_range_ids.length,
  },
  {
    title: "正在复制的（或复制较慢的）",
    extract: (problem) => problem.underreplicated_range_ids.length,
  },
  {
    title: "过度复制的",
    extract: (problem) => problem.overreplicated_range_ids.length,
  },
  {
    title: "停顿等于时钟的",
    extract: (problem) => problem.quiescent_equals_ticking_range_ids.length,
  },
  {
    title: "太大的Raft日志",
    extract: (problem) => problem.raft_log_too_large_range_ids.length,
  },
  {
    title: "总计",
    extract: (problem) => {
      return problem.unavailable_range_ids.length +
        problem.no_raft_leader_range_ids.length +
        problem.no_lease_range_ids.length +
        problem.raft_leader_not_lease_holder_range_ids.length +
        problem.underreplicated_range_ids.length +
        problem.overreplicated_range_ids.length +
        problem.quiescent_equals_ticking_range_ids.length +
        problem.raft_log_too_large_range_ids.length;
    },
  },
  { title: "错误", extract: (problem) => problem.error_message },
];

export default function ConnectionsTable(props: ConnectionsTableProps) {
  const { problemRanges } = props;
  // lastError is already handled by ProblemRanges component.
  if (_.isNil(problemRanges) ||
    _.isNil(problemRanges.data) ||
    !_.isNil(problemRanges.lastError)) {
    return null;
  }
  const { data } = problemRanges;
  const ids = _.chain(_.keys(data.problems_by_node_id))
    .map(id => parseInt(id, 10))
    .sortBy(id => id)
    .value();
  return (
    <div>
      <h2>连接 (via Node {data.node_id})</h2>
      <table className="connections-table">
        <tbody>
          <tr className="connections-table__row connections-table__row--header">
            {
              _.map(connectionTableColumns, (col, key) => (
                <th key={key} className="connections-table__cell connections-table__cell--header">
                  {col.title}
                </th>
              ))
            }
          </tr>
          {
            _.map(ids, id => {
              const rowProblems = data.problems_by_node_id[id];
              const rowClassName = classNames({
                "connections-table__row": true,
                "connections-table__row--warning": !_.isEmpty(rowProblems.error_message),
              });
              return (
                <tr key={id} className={rowClassName}>
                  {
                    _.map(connectionTableColumns, (col, key) => (
                      <td key={key} className="connections-table__cell">
                        {col.extract(rowProblems, id)}
                      </td>
                    ))
                  }
                </tr>
              );
            })
          }
        </tbody>
      </table>
    </div>
  );
}
