  
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
import { RouterState } from "react-router";

import { getDataFromServer } from "src/util/dataFromServer";
import * as protos from "src/js/protos";
import { certificatesRequestKey, refreshCertificates } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { LongToMoment } from "src/util/convert";
import Loading from "src/views/shared/components/loading";

interface CertificatesOwnProps {
  certificates: protos.znbase.server.serverpb.CertificatesResponse;
  lastError: Error;
  refreshCertificates: typeof refreshCertificates;
}

const dateFormat = "Y-MM-DD HH:mm:ss";
const message = "当前是非安全模式下的状态，故而证书不存在，请尝试在安全模式下登录并查看该界面"

type CertificatesProps = CertificatesOwnProps & RouterState;

const emptyRow = (
  <tr className="certs-table__row">
    <th className="certs-table__cell certs-table__cell--header" />
    <td className="certs-table__cell" />
  </tr>
);

function certificatesRequestFromProps(props: CertificatesProps) {
  return new protos.znbase.server.serverpb.CertificatesRequest({
    node_id: props.params[nodeIDAttr],
  });
}

/**
 * Renders the Certificate Report page.
 */
class Certificates extends React.Component<CertificatesProps, {}> {
  refresh(props = this.props) {
    props.refreshCertificates(certificatesRequestFromProps(props));
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: CertificatesProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  renderSimpleRow(header: string, value: string, title: string = "") {
    let realTitle = title;
    if (_.isEmpty(realTitle)) {
      realTitle = value;
    }
    return (
      <tr className="certs-table__row">
        <th className="certs-table__cell certs-table__cell--header">{header}</th>
        <td className="certs-table__cell" title={realTitle}>{value}</td>
      </tr>
    );
  }

  renderMultilineRow(header: string, values: string[]) {
    return (
      <tr className="certs-table__row">
        <th className="certs-table__cell certs-table__cell--header">{header}</th>
        <td className="certs-table__cell" title={_.join(values, "\n")}>
          <ul className="certs-entries-list">
            {
              _.chain(values)
                .sort()
                .map((value, key) => (
                  <li key={key}>
                    {value}
                  </li>
                ))
                .value()
            }
          </ul>
        </td>
      </tr>
    );
  }

  renderTimestampRow(header: string, value: Long) {
    const timestamp = LongToMoment(value).format(dateFormat);
    const title = value + "\n" + timestamp;
    return this.renderSimpleRow(header, timestamp, title);
  }

  renderFields(fields: protos.znbase.server.serverpb.CertificateDetails.IFields, id: number) {
    return [
      this.renderSimpleRow("证书 ID", id.toString()),
      this.renderSimpleRow("发行者", fields.issuer),
      this.renderSimpleRow("主体", fields.subject),
      this.renderTimestampRow("生效日期", fields.valid_from),
      this.renderTimestampRow("到期日期", fields.valid_until),
      this.renderMultilineRow("地址", fields.addresses),
      this.renderSimpleRow("签名算法", fields.signature_algorithm),
      this.renderSimpleRow("公钥", fields.public_key),
      this.renderMultilineRow("密钥用法", fields.key_usage),
      this.renderMultilineRow("扩展密钥用法", fields.extended_key_usage),
    ];
  }

  renderCert(cert: protos.znbase.server.serverpb.ICertificateDetails, key: number) {
    let certType: string;
    switch (cert.type) {
      case protos.znbase.server.serverpb.CertificateDetails.CertificateType.CA:
        certType = "Certificate Authority";
        break;
      case protos.znbase.server.serverpb.CertificateDetails.CertificateType.NODE:
        certType = "Node Certificate";
        break;
      case protos.znbase.server.serverpb.CertificateDetails.CertificateType.CLIENT_CA:
        certType = "Client Certificate Authority";
        break;
      case protos.znbase.server.serverpb.CertificateDetails.CertificateType.CLIENT:
        certType = "Client Certificate";
        break;
      case protos.znbase.server.serverpb.CertificateDetails.CertificateType.UI_CA:
        certType = "UI Certificate Authority";
        break;
      case protos.znbase.server.serverpb.CertificateDetails.CertificateType.UI:
        certType = "UI Certificate";
        break;
      default:
        certType = "Unknown";
    }
    return (
      <table key={key} className="certs-table">
        <tbody>
          {this.renderSimpleRow("类型", certType)}
          {
            _.map(cert.fields, (fields, id) => {
              const result = this.renderFields(fields, id);
              if (id > 0) {
                result.unshift(emptyRow);
              }
              return result;
            })
          }
        </tbody>
      </table>
    );
  }

  renderContent = () => {
    const { certificates } = this.props;
    const nodeID = this.props.params[nodeIDAttr];

    if (_.isEmpty(certificates.certificates)) {
      return (
        <React.Fragment>
          <h2>在节点 {this.props.params[nodeIDAttr]}上没有找到证书</h2>
        </React.Fragment>
      );
    }

    let header: string = null;
    if (_.isNaN(parseInt(nodeID, 10))) {
      header = "本地节点";
    } else {
      header = `节点 ${nodeID}`;
    }

    return (
      <React.Fragment>
        <h2>{header} 证书</h2>
        {
          _.map(certificates.certificates, (cert, key) => (
            this.renderCert(cert, key)
          ))
        }
      </React.Fragment>
    );
  }

  render() {
    let IsUserIn = getDataFromServer().LoginEnabled 
    return (
      <div className="section">
        <Helmet>
          <title>证书 | 调试</title>
        </Helmet>
        <h1>证书</h1>
      {IsUserIn?<section className="section">
          <Loading
            loading={!this.props.certificates}
            error={this.props.lastError}
            render={this.renderContent}
          />
        </section>:message
        }

      </div>
      
    );
  }
}

function mapStateToProps(state: AdminUIState, props: CertificatesProps) {
  const nodeIDKey = certificatesRequestKey(certificatesRequestFromProps(props));
  return {
    certificates: state.cachedData.certificates[nodeIDKey] && state.cachedData.certificates[nodeIDKey].data,
    lastError: state.cachedData.certificates[nodeIDKey] && state.cachedData.certificates[nodeIDKey].lastError,
  };
}

const actions = {
  refreshCertificates,
};

export default connect(mapStateToProps, actions)(Certificates);
