  
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

import React from "react";
import { Helmet } from "react-helmet";
import { Link } from "react-router";
import _ from "lodash";
import { connect } from "react-redux";
import moment from "moment";

import "./events.styl";

import * as protos from "src/js/protos";

import { AdminUIState } from "src/redux/state";
import { refreshEvents } from "src/redux/apiReducers";
import { eventsSelector, eventsValidSelector } from "src/redux/events";
import { LocalSetting } from "src/redux/localsettings";
import { TimestampToMoment } from "src/util/convert";
import { getEventDescription } from "src/util/events";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { SortedTable } from "src/views/shared/components/sortedtable";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";

type Event$Properties = protos.znbase.server.serverpb.EventsResponse.IEvent;

// Number of events to show in the sidebar.
const EVENT_BOX_NUM_EVENTS = 10;

const eventsSortSetting = new LocalSetting<AdminUIState, SortSetting>(
  "events/sort_setting", (s) => s.localSettings,
);

export interface SimplifiedEvent {
   // How long ago the event occurred  (e.g. "10 minutes ago").
  fromNowString: string;
  sortableTimestamp: moment.Moment;
  content: React.ReactNode;
}

class EventSortedTable extends SortedTable<SimplifiedEvent> {}

export interface EventRowProps {
  event: Event$Properties;
}

export function getEventInfo(e: Event$Properties): SimplifiedEvent {
  return {
    fromNowString: TimestampToMoment(e.timestamp).fromNow()
      .replace("second", "sec")
      .replace("minute", "min"),
    content: <span>{ getEventDescription(e) }</span>,
    sortableTimestamp: TimestampToMoment(e.timestamp),
  };
}

export class EventRow extends React.Component<EventRowProps, {}> {
  render() {
    const { event } = this.props;
    const e = getEventInfo(event);
    return <tr>
      <td>
        <ToolTipWrapper text={ e.content }>
          <div className="events__message">{e.content}</div>
        </ToolTipWrapper>
      </td>
      <td><div className="events__timestamp">{e.fromNowString}</div></td>
    </tr>;
  }
}

export interface EventBoxProps {
  events: Event$Properties[];
  // eventsValid is needed so that this component will re-render when the events
  // data becomes invalid, and thus trigger a refresh.
  eventsValid: boolean;
  refreshEvents: typeof refreshEvents;
}

export class EventBoxUnconnected extends React.Component<EventBoxProps, {}> {

  componentWillMount() {
    // Refresh events when mounting.
    this.props.refreshEvents();
  }

  componentWillReceiveProps(props: EventPageProps) {
    // Refresh events when props change.
    props.refreshEvents();
  }

  render() {
    const events = this.props.events;
    return <div className="events">
      <table>
        <tbody>
          {_.map(_.take(events, EVENT_BOX_NUM_EVENTS), (e: Event$Properties, i: number) => {
            return <EventRow event={e} key={i} />;
          })}
          <tr>
            <td className="events__more-link" colSpan={2}><Link to="/events">查看所有事件</Link></td>
          </tr>
        </tbody>
      </table>
    </div>;
  }
}

export interface EventPageProps {
  events: Event$Properties[];
  // eventsValid is needed so that this component will re-render when the events
  // data becomes invalid, and thus trigger a refresh.
  eventsValid: boolean;
  refreshEvents: typeof refreshEvents;
  sortSetting: SortSetting;
  setSort: typeof eventsSortSetting.set;
}

export class EventPageUnconnected extends React.Component<EventPageProps, {}> {
  componentWillMount() {
    // Refresh events when mounting.
    this.props.refreshEvents();
  }

  componentWillReceiveProps(props: EventPageProps) {
    // Refresh events when props change.
    props.refreshEvents();
  }

  render() {
    const { events, sortSetting } = this.props;

    const simplifiedEvents = _.map(events, getEventInfo);

    return <div>
      <Helmet>
        <title>事件</title>
      </Helmet>
      <section className="section section--heading">
        <h1>事件</h1>
      </section>
      <section className="section l-columns">
        <div className="l-columns__left events-table">
          <EventSortedTable
            data={simplifiedEvents}
            sortSetting={sortSetting}
            onChangeSortSetting={(setting) => this.props.setSort(setting)}
            columns={[
              {
                title: "事件",
                cell: (e) => e.content,
              },
              {
                title: "时间戳",
                cell: (e) => e.fromNowString,
                sort: (e) => e.sortableTimestamp,
              },
            ]}
            />
        </div>
      </section>
    </div>;
  }
}

// Connect the EventsList class with our redux store.
const eventBoxConnected = connect(
  (state: AdminUIState) => {
    return {
      events: eventsSelector(state),
      eventsValid: eventsValidSelector(state),
    };
  },
  {
    refreshEvents,
  },
)(EventBoxUnconnected);

// Connect the EventsList class with our redux store.
const eventPageConnected = connect(
  (state: AdminUIState) => {
    return {
      events: eventsSelector(state),
      eventsValid: eventsValidSelector(state),
      sortSetting: eventsSortSetting.selector(state),
    };
  },
  {
    refreshEvents,
    setSort: eventsSortSetting.set,
  },
)(EventPageUnconnected);

export { eventBoxConnected as EventBox };
export { eventPageConnected as EventPage };
