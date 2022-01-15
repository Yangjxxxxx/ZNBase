import React from "react";
import { connect, Dispatch } from "react-redux";
import { Link } from "react-router";
import classNames from "classnames";

import { allNodesHaveLocality } from "src/util/localities";
import {
  instructionsBoxCollapsedSelector, setInstructionsBoxCollapsed,
} from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { nodeStatusesSelector } from "src/redux/nodes";
import { LocalityTier } from "src/redux/localities";
//import * as docsURL from "src/util/docs";
import nodeMapScreenshot from "assets/nodeMapSteps/3-seeMap.png";
import questionMap from "assets/questionMap.svg";
import "./instructionsBox.styl";

interface InstructionsBoxProps {
  allNodesHaveLocality: boolean;
  collapsed: boolean;
  expand: () => void;
  collapse: () => void;
}

class InstructionsBox extends React.Component<InstructionsBoxProps> {
  renderExpanded() {
    const firstTodoDone = this.props.allNodesHaveLocality;

    return (
      <div className="instructions-box instructions-box--expanded">
        <div className="instructions-box-top-bar">
          <div>
            <span className="instructions-box-top-bar__see_nodes">
              在地图上查看你的节点!
            </span>{" "}
            {/* <a
              href={docsURL.enableNodeMap}
              target="_blank"
              className="instructions-box-top-bar__setup_link"
            >
              跟随我们的配置指南
            </a> */}
          </div>
          <span
            className="instructions-box-top-bar__x-out"
            onClick={this.props.collapse}
          >
            ✕
          </span>
        </div>
        <div className="instructions-box-content">
          <ol>
            <li
              className={classNames(
                "instructions-box-content__todo-item",
                { "instructions-box-content__todo-item--done": firstTodoDone },
              )}
            >
              { firstTodoDone ? (<div className="instructions-box-content__todo-check">{"\u2714"}</div>) : null }
              确保集群中的每个节点都有一个 <code>--space</code> 标志.
              (<Link to={"/reports/localities"}>查看节点位置信息</Link>)
            </li>
            <li>
              节点的位置信息将添加到与位置标志对应的 <code>system.locations</code>表中
            </li>
          </ol>
          <div className="instructions-box-content__screenshot">
            <img src={nodeMapScreenshot} />
          </div>
        </div>
      </div>
    );
  }

  renderCollapsed() {
    return (
      <div
        className="instructions-box instructions-box--collapsed"
        onClick={this.props.expand}
      >
        <img src={questionMap} />
      </div>
    );
  }

  render() {
    if (this.props.collapsed) {
      return this.renderCollapsed();
    } else {
      return this.renderExpanded();
    }
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    collapsed: instructionsBoxCollapsedSelector(state),
    allNodesHaveLocality: allNodesHaveLocality(nodeStatusesSelector(state)),
  };
}

function mapDispatchToProps(dispatch: Dispatch<AdminUIState>) {
  return {
    expand: () => dispatch(setInstructionsBoxCollapsed(false)),
    collapse: () => dispatch(setInstructionsBoxCollapsed(true)),
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(InstructionsBox);

// Helper functions.

/**
 * showInstructionBox decides whether to show the instructionBox.
 */
export function showInstructionsBox(showMap: boolean, tiers: LocalityTier[]): boolean {
  const atTopLevel = tiers.length === 0;
  return atTopLevel && !showMap;
}
