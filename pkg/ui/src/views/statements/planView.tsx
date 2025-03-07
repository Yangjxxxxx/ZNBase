  
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

import { znbase } from "src/js/protos";
import IAttr = znbase.sql.ExplainTreePlanNode.IAttr;
import IExplainTreePlanNode = znbase.sql.IExplainTreePlanNode;
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

const WARNING_ICON = (
  <svg className="warning-icon" width="17" height="17" viewBox="0 0 24 22" xmlns="http://www.w3.org/2000/svg">
    <path fill-rule="evenodd" clip-rule="evenodd" d="M15.7798 2.18656L23.4186 15.5005C25.0821 18.4005 22.9761 21.9972 19.6387 21.9972H4.3619C1.02395 21.9972 -1.08272 18.4009 0.582041 15.5005M0.582041 15.5005L8.21987 2.18656C9.89189 -0.728869 14.1077 -0.728837 15.7798 2.18656M13.4002 7.07075C13.4002 6.47901 12.863 5.99932 12.2002 5.99932C11.5375 5.99932 11.0002 6.47901 11.0002 7.07075V13.4993C11.0002 14.0911 11.5375 14.5707 12.2002 14.5707C12.863 14.5707 13.4002 14.0911 13.4002 13.4993V7.07075ZM13.5717 17.2774C13.5717 16.5709 12.996 15.9981 12.286 15.9981C11.5759 15.9981 11.0002 16.5709 11.0002 17.2774V17.2902C11.0002 17.9967 11.5759 18.5695 12.286 18.5695C12.996 18.5695 13.5717 17.9967 13.5717 17.2902V17.2774Z"/>
  </svg>
);
const NODE_ICON = (
  <span className="node-icon">&#x26AC;</span>
);

// FlatPlanNodeAttribute contains a flattened representation of IAttr[].
export interface FlatPlanNodeAttribute {
  key: string;
  values: string[];
  warn: boolean;
}

// FlatPlanNode contains details for the flattened representation of
// IExplainTreePlanNode.
//
// Note that the function that flattens IExplainTreePlanNode returns
// an array of FlatPlanNode (not a single FlatPlanNode). E.g.:
//
//  flattenTree(IExplainTreePlanNode) => FlatPlanNode[]
//
export interface FlatPlanNode {
  name: string;
  attrs: FlatPlanNodeAttribute[];
  children: FlatPlanNode[][];
}

/* ************************* HELPER FUNCTIONS ************************* */

// flattenTree takes a tree representation of a logical plan
// (IExplainTreePlanNode) and flattens any single child paths.
// For example, if an IExplainTreePlanNode was visually displayed
// as:
//
//    root
//       |
//       |___ single_grandparent
//                  |
//                  |____ parent_1
//                  |         |
//                  |         |______ single_child
//                  |
//                  |____ parent_2
//
// Then its FlatPlanNode[] equivalent would be visually displayed
// as:
//
//    root
//       |
//    single_grandparent
//       |
//       |____ parent_1
//       |          |
//       |     single_child
//       |
//       |____ parent_2
//
export function flattenTree(treePlan: IExplainTreePlanNode): FlatPlanNode[] {
  const flattenedPlan: FlatPlanNode[] = [
    {
      "name": treePlan.name,
      "attrs": flattenAttributes(treePlan.attrs),
      "children": [],
    },
  ];

  if (treePlan.children.length === 0) {
    return flattenedPlan;
  }
  const flattenedChildren =
    treePlan.children
      .map( (child) => flattenTree(child));
  if (treePlan.children.length === 1) {
    // Append single child into same list that contains parent node.
    flattenedPlan.push(...flattenedChildren[0]);
  } else {
    // Only add to children property if there are multiple children.
    flattenedPlan[0].children = flattenedChildren;
  }
  return flattenedPlan;
}

// flattenAttributes takes a list of attrs (IAttr[]) and collapses
// all the values for the same key (FlatPlanNodeAttribute). For example,
// if attrs was:
//
// attrs: IAttr[] = [
//  {
//    key: "render",
//    value: "name",
//  },
//  {
//    key: "render",
//    value: "title",
//  },
// ];
//
// The returned FlatPlanNodeAttribute would be:
//
// flattenedAttr: FlatPlanNodeAttribute = {
//  key: "render",
//  value: ["name", "title"],
// };
//
export function flattenAttributes(attrs: IAttr[]|null): FlatPlanNodeAttribute[] {
  if (attrs === null) {
    return [];
  }
  const flattenedAttrsMap: { [key: string]: FlatPlanNodeAttribute } = {};
  attrs.forEach( (attr) => {
    const existingAttr = flattenedAttrsMap[attr.key];
    const warn = warnForAttribute(attr);
    if (!existingAttr) {
      flattenedAttrsMap[attr.key] = {
        key: attr.key,
        values: [attr.value],
        warn: warn,
      };
    } else {
      existingAttr.values.push(attr.value);
      if (warn) {
        existingAttr.warn = true;
      }
    }
  });
  const flattenedAttrs = _.values(flattenedAttrsMap);
  return _.sortBy(flattenedAttrs, (attr) => (
    attr.key === "table" ? "table" : "z" + attr.key
  ));
}

function warnForAttribute(attr: IAttr): boolean {
  if (attr.key === "spans" && attr.value === "ALL") {
    return true;
  }
  return false;
}

// shouldHideNode looks at node name to determine whether we should hide
// node from logical plan tree.
//
// Currently we're hiding `row source to planNode`, which is a node
// generated during execution (e.g. this is an internal implementation
// detail that will add more confusion than help to user). See #34594
// for details.
function shouldHideNode(nodeName: string): boolean {
  if (nodeName === "row source to plan node") {
    return true;
  }
  return false;
}

/* ************************* PLAN NODES ************************* */

interface PlanNodeDetailProps {
  node: FlatPlanNode;
}

class PlanNodeDetails extends React.Component<PlanNodeDetailProps> {
  constructor(props: PlanNodeDetailProps) {
    super(props);
  }

  renderAttributeValues(values: string[]) {
    if (!values.length || !values[0].length) {
      return;
    }
    if (values.length === 1) {
      return <span> = {values[0]}</span>;
    }
    return <span> = [{values.join((", "))}]</span>;
  }

  renderAttribute(attr: FlatPlanNodeAttribute) {
    let attrClassName = "";
    let keyClassName = "nodeAttributeKey";
    if (attr.warn) {
      attrClassName = "warn";
      keyClassName = "";
    }
    return (
      <div key={attr.key} className={attrClassName}>
        {attr.warn && WARNING_ICON}
        <span className={keyClassName}>{attr.key}</span>
        {this.renderAttributeValues(attr.values)}
      </div>
    );
  }

  renderNodeDetails() {
    const node = this.props.node;
    if (node.attrs && node.attrs.length > 0) {
      return (
        <div className="nodeAttributes">
          {node.attrs.map( (attr) => this.renderAttribute(attr))}
        </div>
      );
    }
  }

  render() {
    const node = this.props.node;
    return (
      <div className="nodeDetails">
        {NODE_ICON} <b>{_.capitalize(node.name)}</b>
        {this.renderNodeDetails()}
      </div>
    );
  }
}

interface PlanNodeProps {
  node: FlatPlanNode;
}

class PlanNode extends React.Component<PlanNodeProps> {
  render() {
    if (shouldHideNode(this.props.node.name)) {
      return null;
    }
    const node = this.props.node;
    return (
      <li>
        <PlanNodeDetails
          node={node}
        />
        {node.children && node.children.map( (child) => (
          <PlanNodes
            nodes={child}
          />
        ))
        }
      </li>
    );
  }
}

function PlanNodes(props: {
  nodes: FlatPlanNode[],
}): React.ReactElement<{}> {
  const nodes = props.nodes;
  return (
    <ul>
      {nodes.map( (node) => {
        return <PlanNode node={node}/>;
      })}
    </ul>
  );
}

interface PlanViewProps {
  title: string;
  plan: IExplainTreePlanNode;
}

interface PlanViewState {
  expanded: boolean;
  showExpandDirections: boolean;
}

export class PlanView extends React.Component<PlanViewProps, PlanViewState> {
  private innerContainer: React.RefObject<HTMLDivElement>;
  constructor(props: PlanViewProps) {
    super(props);
    this.state = {
      expanded: false,
      showExpandDirections: true,
    };
    this.innerContainer = React.createRef();
  }

  toggleExpanded = () => {
    this.setState(state => ({
      expanded: !state.expanded,
    }));
  }

  showExpandDirections() {
    // Only show directions to show/hide the full plan if content is longer than its max-height.
    const containerObj = this.innerContainer.current;
    return containerObj.scrollHeight > containerObj.clientHeight;
  }

  componentDidMount() {
    this.setState({ showExpandDirections: this.showExpandDirections() });
  }

  render() {
    const flattenedPlanNodes = flattenTree(this.props.plan);

    const lastSampledHelpText = (
      <React.Fragment>
        {/* If the time from the last sample is greater than 5 minutes, a new
        plan will be sampled. This frequency can be configured
        with the cluster setting{" "} */}
        如果最后一个样本的时间大于5分钟，则将对新计划进行抽样。这个频率可以通过集群设置进行配置。{" "}
        <code>
        <pre style={{ display: "inline-block" }}>
          sql.metrics.statement_details.plan_collection.period
        </pre>
        </code>.
      </React.Fragment>
    );

    return (
      <table className="plan-view-table">
        <thead>
        <tr className="plan-view-table__row--header">
          <th className="plan-view-table__cell">
            {this.props.title}
            <div className="plan-view-table__tooltip">
              <ToolTipWrapper
                text={lastSampledHelpText}>
                <div className="plan-view-table__tooltip-hover-area">
                  <div className="plan-view-table__info-icon">i</div>
                </div>
              </ToolTipWrapper>
            </div>
          </th>
        </tr>
        </thead>
        <tbody>
        <tr className="plan-view-table__row--body">
          <td className="plan-view plan-view-table__cell" style={{ textAlign: "left" }}>
            <div className="plan-view-container">
              <div
                id="plan-view-inner-container"
                ref={this.innerContainer}
                className={this.state.expanded ? "" : "plan-view-container-scroll"}
              >
                <PlanNodes
                  nodes={flattenedPlanNodes}
                />
              </div>
              {this.state.showExpandDirections &&
                <div className="plan-view-container-directions"
                     onClick={() => this.toggleExpanded()}
                >
                  {!this.state.expanded && "See full plan"}
                  {this.state.expanded && "Collapse plan"}
                </div>
              }
            </div>
          </td>
        </tr>
        </tbody>
      </table>
    );
  }
}
