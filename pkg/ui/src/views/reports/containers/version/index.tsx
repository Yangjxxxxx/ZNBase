import * as React  from 'react'
import LicenseType from "src/views/shared/components/licenseType";
import DebugAnnotation from "src/views/shared/components/debugAnnotation";
import { getDataFromServer } from "src/util/dataFromServer";
import InfoBox from "src/views/shared/components/infoBox";
import { Helmet } from "react-helmet";
import "./version.styl"
const NODE_ID = getDataFromServer().NodeID;
const version = getDataFromServer().Tag || "UNKNOWN";
const user = getDataFromServer().LoggedInUser || "无用户信息(非安全模式)"

export  default class Version extends React.Component{
    render(){
        return(
          <div>
<InfoBox>
         <Helmet>
            <title>版本信息</title>
          </Helmet> 
            <h4 className="login-note-box__heading"> 用户信息</h4>
            用户: <span className="version-tag">{ user }</span>  
      </InfoBox>
      <InfoBox>
      <h4 className="login-note-box__heading">版本信息</h4>
            版本: <span className="version-tag">{ version }</span>
        <pre className="login-note-box__sql-command">
          <span className="sql-keyword"><LicenseType /></span>
          <span className="sql-keyword"><DebugAnnotation label="服务器" value={ `n${NODE_ID}` } /></span>
        </pre>
      </InfoBox>
          </div>
        
        )
    }
}
