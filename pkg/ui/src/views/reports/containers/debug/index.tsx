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

import { getDataFromServer } from "src/util/dataFromServer";
import DebugAnnotation from "src/views/shared/components/debugAnnotation";
import InfoBox from "src/views/shared/components/infoBox";
import LicenseType from "src/views/shared/components/licenseType";
import { PanelSection, PanelTitle, PanelPair, Panel } from "src/views/shared/components/panelSection";

import "./debug.styl";
import { DatePicker, Space } from "antd";
import "antd/lib/date-picker/style/css";
import Cookie from 'js-cookie';
import axios from 'axios';

// const COMMUNITY_URL = "https://www.znbaselabs.com/community/";

const NODE_ID = getDataFromServer().NodeID;


function DebugTableLink(props: { name: string, url: string, note?: string }) {
  return (
    <tr className="debug-inner-table__row">
      <td className="debug-inner-table__cell">
        <a className="debug-link" href={props.url}>{props.name}</a>
      </td>
      <td className="debug-inner-table__cell--notes">
        {_.isNil(props.note) ? props.url : props.note}
      </td>
    </tr>
  );
}

const { RangePicker } = DatePicker;


function DebugTableRow(props: { title: string, children?: React.ReactNode }) {
  return (
    <tr className="debug-table__row">
      <th className="debug-table__cell debug-table__cell--header">{props.title}</th>
      <td className="debug-table__cell">
        <table className="debug-inner-table">
          <tbody>
            {props.children}
          </tbody>
        </table>
      </td>
    </tr>
  );
}

function DebugTable(props: { heading: string, children?: React.ReactNode }) {
  return (
    <div>
      <h2>{props.heading}</h2>
      <table className="debug-table">
        <tbody>
          {props.children}
        </tbody>
      </table>
    </div>
  );
}

function DebugPanelLink(props: { name: string, url: string,  note: string }) {
  return (
    <PanelPair>
      <Panel>
        <a href={ props.url }>{ props.name }</a>
        <p>{ props.note }</p>
      </Panel>
      <Panel>
        <div className="debug-url"><div>{ props.url }</div></div>
      </Panel>
    </PanelPair>
  );
}

export default function Debug() {
  return (
    <div className="section">
      <Helmet>
        <title>高级调试</title>
      </Helmet>
      <h1>高级调试</h1>
      <div className="debug-header">
        <InfoBox>
          <p>
          以下页面用于高级监视和故障排除。
          请注意，这些页面是实验性的，没有文档记录。
           {/* 如果发现问题，
           请通过{" "}
            <a href={ COMMUNITY_URL }>这些渠道</a>让我们知道 */}
              </p>
            </InfoBox>

        <div className="debug-header__annotations">
          <LicenseType />
          <DebugAnnotation label="服务器" value={ `n${NODE_ID}` } />
        </div>
      </div>
      <PanelSection>
        <PanelTitle>报告</PanelTitle>
        <DebugPanelLink
          name="自定义时间序列图"
          url="#/debug/chart"
          note="创建时间序列数据的自定义图表"
        />
        <DebugPanelLink
          name="有问题的 Ranges"
          url="#/reports/problemranges"
          note="查看集群中的一些Ranges，它们可能是不可用的、复制不足的，速度较慢的的或存在其他问题的"
        />
        <DebugPanelLink
          name="网络延迟"
          url="#/reports/network"
          note="检查集群中所有节点之间的延迟"
        />
        <DebugPanelLink
          name="数据分配和区域配置"
          url="#/data-distribution"
          note="查看表数据在节点之间的分布并验证区域配置"
        />
        <PanelTitle>配置</PanelTitle>
        <DebugPanelLink
          name="集群设置"
          url="#/reports/settings"
          note="查看所有集群设置"
        />
        <DebugPanelLink
          name="地区"
          url="#/reports/localities"
          note="查看集群的位置 以及 集群中节点的位置"
        />
      </PanelSection>
      <DebugTable heading="专业调试">
        <DebugTableRow title="节点诊断">
          <DebugTableLink name="所有节点" url="#/reports/nodes" />
          <DebugTableLink
            name="按节点ID过滤节点"
            url="#/reports/nodes?node_ids=1,2"
            note="#/reports/nodes?node_ids=[node_id{,node_id...}]"
          />
          <DebugTableLink
            name="按位置过滤节点(支持正则表达式)"
            url="#/reports/nodes?locality=region=us-east"
            note="#/reports/nodes?locality=[regex]"
          />
        </DebugTableRow>
        <DebugTableRow title="存储">
          <DebugTableLink name="在此节点上的存储" url="#/reports/stores/local" />
          <DebugTableLink
            name="在特定节点上的存储"
            url="#/reports/stores/1"
            note="#/reports/stores/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="网络">
          <DebugTableLink
            name="按节点ID过滤的延迟"
            url="#/reports/network?node_ids=1,2"
            note="#/reports/network?node_ids=[node_id{,node_id...}]"
          />
          <DebugTableLink
            name="按地区过滤的延迟(支持正则表达式)"
            url="#/reports/network?locality=region=us-east"
            note="#/reports/network?locality=[regex]"
          />
        </DebugTableRow>
        <DebugTableRow title="安全">
          <DebugTableLink name="该节点上的证书" url="#/reports/certificates/local" />
          <DebugTableLink
            name="特定节点上的证书"
            url="#/reports/certificates/1"
            note="#/reports/certificates/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="有问题的 Ranges">
          <DebugTableLink
            name="特定结点上的有问题的 Ranges"
            url="#/reports/problemranges/local"
            note="#/reports/problemranges/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Ranges">
          <DebugTableLink
            name="Range 状态"
            url="#/reports/range/1"
            note="#/reports/range/[range_id]"
          />
          <DebugTableLink name="Raft 信息" url="#/raft/messages/all" />
          <DebugTableLink name="所有range的raft" url="#/raft/ranges" />
        </DebugTableRow>
      </DebugTable>
      <DebugTable heading="追踪和分析端点(仅本地节点)">
        <DebugTableRow title="追踪">
          <DebugTableLink name="要求" url="/debug/requests" />
          <DebugTableLink name="事件" url="/debug/events" />
          <DebugTableLink
            name="日志"
            url="/debug/logspy?count=1&amp;duration=10s&amp;grep=."
            note="/debug/logspy?count=[count]&amp;duration=[duration]&amp;grep=[regexp]"
          />
        </DebugTableRow>
        <DebugTableRow title="将要入队的 Range">
          <DebugTableLink
            name="通过内部队列运行Range"
            url="#/debug/enqueue_range"
            note="#/debug/enqueue_range"
          />
        </DebugTableRow>
        <DebugTableRow title="stopper ">
          <DebugTableLink name="活动任务" url="/debug/stopper" />
        </DebugTableRow>
        <DebugTableRow title="分析UI / pprof">
          <DebugTableLink name="堆" url="/debug/pprof/ui/heap/" />
          <DebugTableLink name="用户资料" url="/debug/pprof/ui/profile/?seconds=5" />
          <DebugTableLink name="用户资料 (带标签)" url="/debug/pprof/ui/profile/?seconds=5&amp;labels=true" />
          <DebugTableLink name="块" url="/debug/pprof/ui/block/" />
          <DebugTableLink name="Mutex" url="/debug/pprof/ui/mutex/" />
          <DebugTableLink name="线程创建" url="/debug/pprof/ui/threadcreate/" />
          <DebugTableLink name="协程" url="/debug/pprof/ui/goroutine/" />
        </DebugTableRow>
        <DebugTableRow title="协程">
          <DebugTableLink name="用户界面" url="/debug/pprof/goroutineui" />
          <DebugTableLink name="用户界面(数量)" url="/debug/pprof/goroutineui?sort=count" />
          <DebugTableLink name="用户界面(等待)" url="/debug/pprof/goroutineui?sort=wait" />
          <DebugTableLink name="Raw" url="/debug/pprof/goroutine?debug=2" />
        </DebugTableRow>
        <DebugTableRow title="运行时追踪">
          <DebugTableLink name="追踪" url="/debug/pprof/trace?debug=1" />
        </DebugTableRow>
      </DebugTable>
      <DebugTable heading="原始状态端点（JSON）">
        <DebugTableRow title="日志（仅单节点）">
          <DebugTableLink
            name="在特定节点上"
            url="/_status/logs/local"
            note="/_status/logs/[node_id]"
          />
          <DebugTableLink
            name="日志文件"
            url="/_status/logfiles/local"
            note="/_status/logfiles/[node_id]"
          />
          <DebugTableLink
            name="特定的日志文件"
            url="/_status/logfiles/local/znbase.log"
            note="/_status/logfiles/[node_id]/[filename]"
          />
        </DebugTableRow>
        <DebugTableRow title="指标">
          <DebugTableLink name="变量" url="/debug/metrics" />
          <DebugTableLink name="Prometheus" url="/_status/vars" />
        </DebugTableRow>
        <DebugTableRow title="节点状态">
          <DebugTableLink
            name="所有节点"
            url="/_status/nodes"
            note="/_status/nodes"
          />
          <DebugTableLink
            name="单个节点状态"
            url="/_status/nodes/local"
            note="/_status/nodes/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Hot Ranges">
          <DebugTableLink
            name="所有节点"
            url="/_status/hotranges"
            note="/_status/hotranges"
          />
          <DebugTableLink
            name="单个节点 ranges"
            url="/_status/hotranges?node_id=local"
            note="/_status/hotranges?node_id=[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="特定单节点">
          <DebugTableLink
            name="存储"
            url="/_status/stores/local"
            note="/_status/stores/[node_id]"
          />
          <DebugTableLink
            name="Gossip"
            url="/_status/gossip/local"
            note="/_status/gossip/[node_id]"
          />
          <DebugTableLink
            name="Ranges"
            url="/_status/ranges/local"
            note="/_status/ranges/[node_id]"
          />
          <DebugTableLink
            name="堆栈"
            url="/_status/stacks/local"
            note="/_status/stacks/[node_id]"
          />
          <DebugTableLink
            name="引擎统计"
            url="/_status/enginestats/local"
            note="/_status/enginestats/[node_id]"
          />
          <DebugTableLink
            name="证书"
            url="/_status/certificates/local"
            note="/_status/certificates/[node_id]"
          />
          <DebugTableLink
            name="诊断报告数据"
            url="/_status/diagnostics/local"
            note="/_status/diagnostics/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Sessions">
          <DebugTableLink name="本地 Sessions" url="/_status/local_sessions" />
          <DebugTableLink name="所有 Sessions" url="/_status/sessions" />
        </DebugTableRow>
        <DebugTableRow title="集群范围">
          <DebugTableLink name="Raft" url="/_status/raft" />
          <DebugTableLink
            name="Range"
            url="/_status/range/1"
            note="/_status/range/[range_id]"
          />
          <DebugTableLink name="Range 日志" url="/_admin/v1/rangelog?limit=100" />
          <DebugTableLink
            name="特定节点的range日志"
            url="/_admin/v1/rangelog/1?limit=100"
            note="/_admin/v1/rangelog/[range_id]?limit=100"
          />
        </DebugTableRow>
        <DebugTableRow title="分配者">
          <DebugTableLink
            name="模拟分配器在特定节点上的运行"
            url="/_status/allocator/node/local"
            note="/_status/allocator/node/[node_id]"
          />
          <DebugTableLink
            name="模拟分配器在特定范围内运行"
            url="/_status/allocator/range/1"
            note="/_status/allocator/range/[range_id]"
          />
        </DebugTableRow>
      <DebugTableRow title="报表">
        <DebugTableLink
            name="生成当前时间报表"
            url="/_status/report"
            note="/_status/report"
        />
        <DebugTableLink
            name="报表目录"
            url="/reportFile"
        />
        <Space direction="vertical" size={12}>
          <RangePicker showTime
          onChange={
            val=>{
              Cookie.set('selectedDate',JSON.stringify(val))
            }
          }
          />
        </Space>
        <button onClick={
          ()=>{
            let arr=JSON.parse(Cookie.get('selectedDate'));

            if(arr.length>0){
              let startTime=arr[0];
              let endTime=arr[1];

              axios.post('/ts/report_query', {
                start_nanos: startTime,
                end_nanos: endTime
              }).then(function (response) {
                    console.log(response);
                  })
            }
          }
        }>时序报表输出</button>
      </DebugTableRow>
      </DebugTable>
      <DebugTable heading="用户界面调试">
        <DebugTableRow title="Redux 状态">
          <DebugTableLink
            name="导出UI的Redux状态"
            url="#/debug/redux"
          />
        </DebugTableRow>
      </DebugTable>
    </div>
  );
}
