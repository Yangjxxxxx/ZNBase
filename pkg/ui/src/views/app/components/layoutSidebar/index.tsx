import classNames from "classnames";
import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import { Link } from "react-router";

import { AdminUIState } from "src/redux/state";
import { selectLoginState, LoginState, doLogout } from "src/redux/login";
import { LOGOUT_PAGE } from "src/routes/login";
import { znbaseIcon } from "src/views/shared/components/icons";
import { trustIcon } from "src/util/trust";

import homeIcon from "!!raw-loader!assets/sidebarIcons/home.svg";
import metricsIcon from "!!raw-loader!assets/sidebarIcons/metrics.svg";
import databasesIcon from "!!raw-loader!assets/sidebarIcons/databases.svg";
import jobsIcon from "!!raw-loader!assets/sidebarIcons/jobs.svg";
import statementsIcon from "!!raw-loader!assets/sidebarIcons/statements.svg";
import unlockedIcon from "!!raw-loader!assets/unlocked.svg";
import gearIcon from "!!raw-loader!assets/sidebarIcons/gear.svg";

import "./navigation-bar.styl";

interface IconLinkProps {
  icon: string;
  title?: string;
  to: string;
  activeFor?: string | string[];
  className?: string;
}

/**
 * IconLink creats a react router Link which contains both a graphical icon and
 * a string title.
 */
class IconLink extends React.Component<IconLinkProps, {}> {
  static defaultProps: Partial<IconLinkProps> = {
    className: "normal",
    activeFor: [],
  };

  static contextTypes = {
    router: PropTypes.object,
  };

  render() {
    const { icon, title, to, activeFor, className } = this.props;

    const router = this.context.router;
    const linkRoutes = [to].concat(activeFor);
    const isActive = _.some(linkRoutes, (route) => router.isActive(route, false));
    const linkClassName = classNames("icon-link", { active: isActive });
    return (
      <li className={className} >
        <Link to={to} className={linkClassName}>
          <div className="image-container"
               dangerouslySetInnerHTML={trustIcon(icon)}/>
          <div>{title}</div>
        </Link>
      </li>
    );
  }
}

interface LoginIndicatorProps {
  loginState: LoginState;
  handleLogout: () => null;
}

function LoginIndicator({ loginState, handleLogout }: LoginIndicatorProps) {
  if (!loginState.useLogin()) {
    return null;
  }

  if (!loginState.loginEnabled()) {
    return (
      <li className="login-indicator login-indicator--insecure">
        <div className="image-container"
             dangerouslySetInnerHTML={trustIcon(unlockedIcon)}/>
        <div>非安全模式</div>
      </li>
    );
  }

  const user = loginState.loggedInUser();
  if (user == null) {
    return null;
  }

  return (
    <li className="login-indicator">
      <Link to={LOGOUT_PAGE} onClick={handleLogout}>
        <div
          className="login-indicator__initial"
          title={`以 ${user}登入`}
        >
          {user[0]}
        </div>
        <div>注销</div>
      </Link>
    </li>
  );
}

// tslint:disable-next-line:variable-name
const LoginIndicatorConnected = connect(
  (state: AdminUIState) => ({
    loginState: selectLoginState(state),
  }),
  (dispatch) => ({
    handleLogout: () => {
      dispatch(doLogout());
    },
  }),
)(LoginIndicator);

/**
 * Sidebar represents the static navigation sidebar available on all pages. It
 * displays a number of graphic icons representing available pages; the icon of
 * the page which is currently active will be highlighted.
 */
export default class Sidebar extends React.Component {
  static contextTypes = {
    router: PropTypes.object,
  };

  render() {
    return (
      <nav className="navigation-bar">
        <ul className="navigation-bar__list">
          <IconLink to="/overview" icon={homeIcon} title="概览" activeFor="/node" />
          <IconLink to="/metrics" icon={metricsIcon} title="指标" />
          <IconLink to="/databases" icon={databasesIcon} title="数据库" activeFor="/database" />
          <IconLink to="/statements" icon={statementsIcon} title="语句" activeFor="/statement" />
          <IconLink to="/jobs" icon={jobsIcon} title="任务" />
        </ul>
        <ul className="navigation-bar__list navigation-bar__list--bottom">
          <IconLink to="/debug" icon={gearIcon} className="normal debug-pages-link" activeFor={["/reports", "/data-distribution", "/raft"]} />
          <LoginIndicatorConnected />
          <IconLink to="/version" icon={znbaseIcon} className="znbase" />
        </ul>
      </nav>
    );
  }
}
