
import classNames from "classnames";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { withRouter, WithRouterProps } from "react-router";

import { doLogin, LoginAPIState } from "src/redux/login";
import { AdminUIState } from "src/redux/state";
import { getDataFromServer } from "src/util/dataFromServer";
import * as docsURL from "src/util/docs";
import { trustIcon } from "src/util/trust";
import InfoBox from "src/views/shared/components/infoBox";

import logo from "assets/znbase.png";
import docsIcon from "!!raw-loader!assets/docs.svg";
//import newFeatureIcon from "!!raw-loader!assets/paperPlane.svg";
import "./loginPage.styl";

const version = getDataFromServer().Tag || "UNKNOWN";

interface LoginPageProps {
  loginState: LoginAPIState;
  handleLogin: (username: string, password: string) => Promise<void>;
}

interface LoginPageState {
  username: string;
  password: string;
}

class LoginPage extends React.Component<LoginPageProps & WithRouterProps, LoginPageState> {
  constructor(props: LoginPageProps & WithRouterProps) {
    super(props);
    this.state = {
      username: "",
      password: "",
    };
    // TODO(vilterp): focus username field on mount
  }

  handleUpdateUsername = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      username: evt.currentTarget.value,
    });
  }

  handleUpdatePassword = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      password: evt.currentTarget.value,
    });
  }

  handleSubmit = (evt: React.FormEvent<any>) => {
    evt.preventDefault();

    this.props.handleLogin(this.state.username, this.state.password)
      .then(() => {
        const { location, router } = this.props;
        if (location.query && location.query.redirectTo) {
          router.push(location.query.redirectTo);
        } else {
          router.push("/");
        }
      });
  }

  renderError() {
    const { error } = this.props.loginState;

    if (!error) {
      return null;
    }

    // let message = "Invalid username or password.";
    let message = "无效的用户名或密码。";
    if (error.message !== "Unauthorized") {
        message = error.message;
    }
    return (
      <div className="login-page__error">无法登录: { message }</div>
    );
  }

  render() {
    const inputClasses = classNames("input-text", {
      "input-text--error": !!this.props.loginState.error,
    });

    return (
      <div className="login-page">
        <Helmet>
          <title>登录</title>
        </Helmet>
        <div className="content">
          <section className="section login-page__info">
            {/* <img className="logo" alt="Inspur" src={logo} /> */}
            {/* <div className="login-new-feature">
              <div className="login-new-feature-heading">
                <span dangerouslySetInnerHTML={trustIcon(newFeatureIcon)} />
                <span className="login-new-feature-heading__text"> v2.1的新特性</span>
              </div>
              <p className="login-new-feature__blurb">
               
                要求有密码的用户在安全集群处登入UI
              </p>
            </div> */}
            <InfoBox>
              <h4 className="login-note-box__heading">提示:</h4>
              <p className="login-note-box__blurb">
                {/* Create a user with this SQL command: */}
                以该SQL命令创建一个用户
              </p>
              <pre className="login-note-box__sql-command">
                <span className="sql-keyword">CREATE USER</span>
                {" "}inspur{" "}
                <span className="sql-keyword">WITH PASSWORD</span>
                {" "}
                <span className="sql-string">'ZNbase@2020'</span>
                <span className="sql-keyword">;</span>
              </pre>
              <p className="aside">
                <a href={docsURL.adminUILogin} className="login-docs-link" target="_blank">
                  {/* <span className="login-docs-link__icon" dangerouslySetInnerHTML={trustIcon(docsIcon)} /> */}
                  {/* <span className="login-docs-link__text">Read more about configuring login</span> */}
                  {/* <span className="login-docs-link__text">阅读更多有关登录的信息</span> */}
                </a>
              </p>
            </InfoBox>
          </section>
          <section className="section login-page__form">
            <p className="version">
              版本: <span className="version-tag">{ version }</span>
            </p>
            <div className="form-container">
              {/* <h1 className="heading">Log in to the Web UI</h1> */}
              <h1 className="heading">登录数据库控制台</h1>
              {this.renderError()}
              <form onSubmit={this.handleSubmit} className="form-internal" method="post">
                <input
                  type="text"
                  name="username"
                  className={inputClasses}
                  onChange={this.handleUpdateUsername}
                  value={this.state.username}
                  placeholder="用户名"
                />
                <input
                  type="password"
                  name="password"
                  className={inputClasses}
                  onChange={this.handleUpdatePassword}
                  value={this.state.password}
                  placeholder="密码"
                />
                <input
                  type="submit"
                  className="submit-button"
                  disabled={this.props.loginState.inProgress}
                  value={this.props.loginState.inProgress ? "正在登录" : "登录"}
                />
              </form>
            </div>
          </section>
        </div>
      </div>
    );
  }
}

// tslint:disable-next-line:variable-name
const LoginPageConnected = connect(
  (state: AdminUIState) => {
    return {
      loginState: state.login,
    };
  },
  (dispatch) => ({
    handleLogin: (username: string, password: string) => {
      return dispatch(doLogin(username, password));
    },
  }),
)(withRouter(LoginPage));

export default LoginPageConnected;
