  
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
import moment from "moment";
import { Line } from "rc-progress";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";

import { znbase } from "src/js/protos";
import { CachedDataReducerState, jobsKey, refreshJobs } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { TimestampToMoment } from "src/util/convert";
import * as docsURL from "src/util/docs";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import Loading from "src/views/shared/components/loading";
import { PageConfig, PageConfigItem } from "src/views/shared/components/pageconfig";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { ColumnDescriptor, SortedTable } from "src/views/shared/components/sortedtable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import { trustIcon } from "src/util/trust";
import "./index.styl";

import succeededIcon from "!!raw-loader!assets/jobStatusIcons/checkMark.svg";
import failedIcon from "!!raw-loader!assets/jobStatusIcons/exclamationPoint.svg";

import Job = znbase.server.serverpb.JobsResponse.IJob;
import JobType = znbase.sql.jobs.jobspb.Type;
import JobsRequest = znbase.server.serverpb.JobsRequest;
import JobsResponse = znbase.server.serverpb.JobsResponse;

const statusOptions = [
  { value: "", label: "所有" },
  { value: "pending", label: "挂起" },
  { value: "running", label: "运行中" },
  { value: "paused", label: "暂停" },
  { value: "canceled", label: "取消" },
  { value: "succeeded", label: "成功" },
  { value: "failed", label: "失败" },
];

const statusSetting = new LocalSetting<AdminUIState, string>(
  "jobs/status_setting", s => s.localSettings, statusOptions[0].value,
);

const typeOptions = [
  { value: JobType.UNSPECIFIED.toString(), label: "所有" },
  { value: JobType.DUMP.toString(), label: "备份" },
  { value: JobType.RESTORE.toString(), label: "还原" },
  { value: JobType.IMPORT.toString(), label: "导入" },
  { value: JobType.SCHEMA_CHANGE.toString(), label: "改变schema" },
  { value: JobType.CHANGEFEED.toString(), label: "CDC"},
  { value: JobType.CREATE_STATS.toString(), label: "用户statistics"},
  { value: JobType.AUTO_CREATE_STATS.toString(), label: "系统statisics"},
];

const typeSetting = new LocalSetting<AdminUIState, number>(
  "jobs/type_setting", s => s.localSettings, JobType.UNSPECIFIED,
);

const showOptions = [
  { value: "50", label: "最近50个" },
  { value: "0", label: "全部" },
];

const showSetting = new LocalSetting<AdminUIState, string>(
  "jobs/show_setting", s => s.localSettings, showOptions[0].value,
);

// Moment cannot render durations (moment/moment#1048). Hack it ourselves.
const formatDuration = (d: moment.Duration) =>
  [Math.floor(d.asHours()).toFixed(0), d.minutes(), d.seconds()]
    .map(c => ("0" + c).slice(-2))
    .join(":");

const JOB_STATUS_SUCCEEDED = "succeeded";
const JOB_STATUS_FAILED = "failed";
const JOB_STATUS_CANCELED = "canceled";
const JOB_STATUS_PENDING = "pending";
const JOB_STATUS_PAUSED = "paused";
const JOB_STATUS_RUNNING = "running";

const STATUS_ICONS: { [state: string]: string } = {
  [JOB_STATUS_SUCCEEDED]: succeededIcon,
  [JOB_STATUS_FAILED]: failedIcon,
};

class JobStatusCell extends React.Component<{ job: Job }, {}> {
  is(...statuses: string[]) {
    return statuses.indexOf(this.props.job.status) !== -1;
  }

  renderProgress() {
    if (this.is(JOB_STATUS_SUCCEEDED, JOB_STATUS_FAILED, JOB_STATUS_CANCELED)) {
      return (
        <div className="jobs-table__status">
          {this.props.job.status in STATUS_ICONS
            ? <div
                className="jobs-table__status-icon"
                dangerouslySetInnerHTML={trustIcon(STATUS_ICONS[this.props.job.status])}
              />
            : null}
          {this.props.job.status}
        </div>
      );
    }
    const percent = this.props.job.fraction_completed * 100;
    return (
      <div>
        {this.props.job.running_status
          ? <div className="jobs-table__running-status">{this.props.job.running_status}</div>
          : null}
        <Line
          percent={percent}
          strokeWidth={10}
          trailWidth={10}
          className="jobs-table__progress-bar"
        />
        <span title={percent.toFixed(3) + "%"}>{percent.toFixed(1) + "%"}</span>
      </div>
    );
  }

  renderDuration() {
    const started = TimestampToMoment(this.props.job.started);
    const finished = TimestampToMoment(this.props.job.finished);
    const modified = TimestampToMoment(this.props.job.modified);
    if (this.is(JOB_STATUS_PENDING, JOB_STATUS_PAUSED)) {
      return _.capitalize(this.props.job.status);
    } else if (this.is(JOB_STATUS_RUNNING)) {
      const fractionCompleted = this.props.job.fraction_completed;
      if (fractionCompleted > 0) {
        const duration = modified.diff(started);
        const remaining = duration / fractionCompleted - duration;
        return formatDuration(moment.duration(remaining)) + " remaining";
      }
    } else if (this.is(JOB_STATUS_SUCCEEDED)) {
      return "Duration: " + formatDuration(moment.duration(finished.diff(started)));
    }
  }

  renderFractionCompleted() {
    return (
      <div>
        {this.renderProgress()}
        <span className="jobs-table__duration">{this.renderDuration()}</span>
      </div>
    );
  }

  renderHighwater() {
    const highwater = this.props.job.highwater_timestamp;
    const tooltip = this.props.job.highwater_decimal;
    let highwaterMoment = moment(highwater.seconds.toNumber() * 1000);
    // It's possible due to client clock skew that this timestamp could be in
    // the future. To avoid confusion, set a maximum bound of now.
    const now = moment();
    if (highwaterMoment.isAfter(now)) {
      highwaterMoment = now;
    }
    return (
      <ToolTipWrapper text={`System Time: ${tooltip}`}>
        High-water Timestamp: {highwaterMoment.fromNow()}
      </ToolTipWrapper>
    );
  }

  render() {
    if (this.props.job.highwater_timestamp) {
      return this.renderHighwater();
    }
    return this.renderFractionCompleted();
  }
}

class JobsSortedTable extends SortedTable<Job> {}

const jobsTableColumns: ColumnDescriptor<Job>[] = [
  {
    title: "ID",
    cell: job => String(job.id),
    sort: job => job.id,
  },
  {
    title: "描述",
    cell: job => {
      // If a [SQL] job.statement exists, it means that job.description
      // is a human-readable message. Otherwise job.description is a SQL
      // statement.
      const additionalStyle = (job.statement ? "" : " jobs-table__cell--sql");
      return <div className={`jobs-table__cell--description${additionalStyle}`}>{job.description}</div>;
    },
    sort: job => job.description,
  },
  {
    title: "用户",
    cell: job => job.username,
    sort: job => job.username,
  },
  {
    title: "创建时间",
    cell: job => TimestampToMoment(job.created).fromNow(),
    sort: job => TimestampToMoment(job.created).valueOf(),
  },
  {
    title: "状态",
    cell: job => <JobStatusCell job={job} />,
    sort: job => job.fraction_completed,
  },
];

const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "jobs/sort_setting",
  s => s.localSettings,
  { sortKey: 3 /* creation time */, ascending: false },
);

interface JobsTableProps {
  sort: SortSetting;
  status: string;
  show: string;
  type: number;
  setSort: (value: SortSetting) => void;
  setStatus: (value: string) => void;
  setShow: (value: string) => void;
  setType: (value: JobType) => void;
  refreshJobs: typeof refreshJobs;
  jobs: CachedDataReducerState<JobsResponse>;
}

const titleTooltip = (
  <span>
    {/* Some jobs can be paused or canceled through SQL. For details, view the docs */}
    某些事物可以通过SQL暂停或取消。
    有关详细信息，请查看文档
    在<a href={docsURL.pauseJob} target="_blank"><code>PAUSE JOB</code></a>{" "}
    以及 <a href={docsURL.cancelJob} target="_blank"><code>CANCEL JOB</code></a>{" "}
    的语句
  </span>
);

class JobsTable extends React.Component<JobsTableProps> {
  refresh(props = this.props) {
    props.refreshJobs(new JobsRequest({
      status: props.status,
      type: props.type,
      limit: parseInt(props.show, 10),
    }));
  }

  componentWillMount() {
    this.refresh();
  }

  componentWillReceiveProps(props: JobsTableProps) {
    this.refresh(props);
  }

  onStatusSelected = (selected: DropdownOption) => {
    this.props.setStatus(selected.value);
  }

  onTypeSelected = (selected: DropdownOption) => {
    this.props.setType(parseInt(selected.value, 10));
  }

  onShowSelected = (selected: DropdownOption) => {
    this.props.setShow(selected.value);
  }

  renderJobExpanded = (job: Job) => {
    return (
      <div>
        <h3>语句</h3>
        <pre className="job-detail">{job.statement || job.description}</pre>

        {job.status === "failed"
          ? [
              <h3>错误i</h3>,
              <pre className="job-detail">{job.error}</pre>,
            ]
          : null}
      </div>
    );
  }

  renderTable = () => {
    const jobs = this.props.jobs.data.jobs;
    if (_.isEmpty(jobs)) {
      return <div className="no-results"><h2>无结果</h2></div>;
    }
    return (
      <JobsSortedTable
        data={jobs}
        sortSetting={this.props.sort}
        onChangeSortSetting={this.props.setSort}
        className="jobs-table"
        rowClass={job => "jobs-table__row--" + job.status}
        columns={jobsTableColumns}
        expandableConfig={{
          expandedContent: this.renderJobExpanded,
          expansionKey: (job) => job.id.toString(),
        }}
      />
    );
  }

  render() {
    return (
      <div className="jobs-page">
        <Helmet>
          <title>任务</title>
        </Helmet>
        <section className="section">
          <h1>
            任务
            <div className="section-heading__tooltip">
              <ToolTipWrapper text={titleTooltip}>
                <div className="section-heading__tooltip-hover-area">
                  <div className="section-heading__info-icon">i</div>
                </div>
              </ToolTipWrapper>
            </div>
          </h1>
        </section>
        <div>
          <PageConfig>
            <PageConfigItem>
              <Dropdown
                title="状态"
                options={statusOptions}
                selected={this.props.status}
                onChange={this.onStatusSelected}
              />
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown
                title="类型"
                options={typeOptions}
                selected={this.props.type.toString()}
                onChange={this.onTypeSelected}
              />
            </PageConfigItem>
            <PageConfigItem>
              <Dropdown
                title="展示"
                options={showOptions}
                selected={this.props.show}
                onChange={this.onShowSelected}
              />
            </PageConfigItem>
          </PageConfig>
        </div>
        <section className="单元">
          <Loading
            loading={!this.props.jobs || !this.props.jobs.data}
            error={this.props.jobs && this.props.jobs.lastError}
            render={this.renderTable}
          />
        </section>
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState) => {
  const sort = sortSetting.selector(state);
  const status = statusSetting.selector(state);
  const show = showSetting.selector(state);
  const type = typeSetting.selector(state);
  const key = jobsKey(status, type, parseInt(show, 10));
  const jobs = state.cachedData.jobs[key];
  return {
    sort, status, show, type, jobs,
  };
};

const actions = {
  setSort: sortSetting.set,
  setStatus: statusSetting.set,
  setShow: showSetting.set,
  setType: typeSetting.set,
  refreshJobs,
};

export default connect(mapStateToProps, actions)(JobsTable);
