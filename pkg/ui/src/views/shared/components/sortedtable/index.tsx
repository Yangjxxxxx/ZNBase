  
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
import _ from "lodash";
import * as Long from "long";
import { Moment } from "moment";
import { createSelector } from "reselect";

import { SortableTable, SortableColumn, SortSetting } from "src/views/shared/components/sortabletable";
import { ExpandableConfig } from "src/views/shared/components/sortabletable";

/**
 * ColumnDescriptor is used to describe metadata about an individual column
 * in a SortedTable.
 */
export interface ColumnDescriptor<T> {
  // Title string that should appear in the header column.
  title: React.ReactNode;
  // Function which generates the contents of an individual cell in this table.
  cell: (obj: T) => React.ReactNode;
  // Function which returns a value that can be used to sort the collection of
  // objects. This will be used to sort the table according to the data in
  // this column.
  // TODO(vilterp): using an "Ordered" typeclass here would be nice;
  // not sure how to do that in TypeScript.
  sort?: (obj: T) => string | number | Long | Moment;
  // Function that generates a "rollup" value for this column from all objects
  // in a collection. This is used to display an appropriate "total" value for
  // each column.
  rollup?: (objs: T[]) => React.ReactNode;
  // className to be applied to the td elements in this column.
  className?: string;
}

/**
 * SortedTableProps describes the properties expected by a SortedTable
 * component.
 */
interface SortedTableProps<T> {
  // The data which should be displayed in the table. This data may be sorted
  // by this component before display.
  data: T[];
  // Description of columns to display.
  columns: ColumnDescriptor<T>[];
  // sortSetting specifies how data should be sorted in this table, by
  // specifying a column id and a direction.
  sortSetting: SortSetting;
  // Callback that should be invoked when the user want to change the sort
  // setting.
  onChangeSortSetting?: { (ss: SortSetting): void };
  // className to be applied to the table element.
  className?: string;
  // A function that returns the class to apply to a given row.
  rowClass?: (obj: T) => string;

  // expandableConfig, if provided, makes each row in the table "expandable",
  // i.e. each row has an expand/collapse arrow on its left, and renders
  // a full-width area below it when expanded.
  expandableConfig?: {
    // expandedContent returns the content for a row's full-width expanded
    // section, given the object that row represents.
    expandedContent: (obj: T) => React.ReactNode;
    // expansionKey returns a key used to uniquely identify a row for the
    // purposes of tracking whether it's expanded or not.
    expansionKey: (obj: T) => string;
  };
}

interface SortedTableState {
  expandedRows: Set<string>;
}

/**
 * SortedTable displays data rows in a table which can be sorted by the values
 * in a single column. Unsorted row data is passed to SortedTable along with
 * a SortSetting; SortedTable uses a selector to sort the data for display.
 *
 * SortedTable also computes optional "rollup" values for each column; a rollup
 * is a total value that is computed for a column based on all available rows.
 *
 * SortedTable should be preferred over the lower-level SortableTable when
 * all data rows to be displayed are available locally on the client side.
 */
export class SortedTable<T> extends React.Component<SortedTableProps<T>, SortedTableState> {
  static defaultProps: Partial<SortedTableProps<any>> = {
    rowClass: (_obj: any) => "",
  };

  rollups = createSelector(
    (props: SortedTableProps<T>) => props.data,
    (props: SortedTableProps<T>) => props.columns,
    (data: T[], columns: ColumnDescriptor<T>[]) => {
      return _.map(columns, (c): React.ReactNode => {
        if (c.rollup) {
          return c.rollup(data);
        }
        return undefined;
      });
    },
  );

  sorted = createSelector(
    (props: SortedTableProps<T>) => props.data,
    (props: SortedTableProps<T>) => props.sortSetting,
    (props: SortedTableProps<T>) => props.columns,
    (data: T[], sortSetting: SortSetting, columns: ColumnDescriptor<T>[]): T[] => {
      if (!sortSetting) {
        return data;
      }
      const sortColumn = columns[sortSetting.sortKey];
      if (!sortColumn || !sortColumn.sort) {
        return data;
      }
      return _.orderBy(data, sortColumn.sort, sortSetting.ascending ? "asc" : "desc");
    },
  );

  /**
   * columns is a selector which computes the input columns to the underlying
   * sortableTable.
   */
  columns = createSelector(
    this.sorted,
    this.rollups,
    (props: SortedTableProps<T>) => props.columns,
    (sorted: T[], rollups: React.ReactNode[], columns: ColumnDescriptor<T>[]) => {
      return _.map(columns, (cd, ii): SortableColumn => {
        return {
          title: cd.title,
          cell: (index) => cd.cell(sorted[index]),
          sortKey: cd.sort ? ii : undefined,
          rollup: rollups[ii],
          className: cd.className,
        };
      });
    });

  rowClass = createSelector(
    this.sorted,
    (props: SortedTableProps<T>) => props.rowClass,
    (sorted: T[], rowClass: (obj: T) => string) => {
      return (index: number) => rowClass(sorted[index]);
    },
  );

  // TODO(vilterp): use a LocalSetting instead so the expansion state
  // will persist if the user navigates to a different page and back.
  state: SortedTableState = {
    expandedRows: new Set<string>(),
  };

  getItemAt(rowIndex: number): T {
    const sorted = this.sorted(this.props);
    return sorted[rowIndex];
  }

  getKeyAt(rowIndex: number): string {
    return this.props.expandableConfig.expansionKey(this.getItemAt((rowIndex)));
  }

  onChangeExpansion = (rowIndex: number, expanded: boolean) => {
    const key = this.getKeyAt(rowIndex);
    const expandedRows = this.state.expandedRows;
    if (expanded) {
      expandedRows.add(key);
    } else {
      expandedRows.delete(key);
    }
    this.setState({
      expandedRows: expandedRows,
    });
  }

  rowIsExpanded = (rowIndex: number): boolean => {
    const key = this.getKeyAt(rowIndex);
    return this.state.expandedRows.has(key);
  }

  expandedContent = (rowIndex: number): React.ReactNode => {
    const item = this.getItemAt(rowIndex);
    return this.props.expandableConfig.expandedContent(item);
  }

  render() {
    const { data, sortSetting, onChangeSortSetting } = this.props;

    let expandableConfig: ExpandableConfig = null;
    if (this.props.expandableConfig) {
      expandableConfig = {
        expandedContent: this.expandedContent,
        rowIsExpanded: this.rowIsExpanded,
        onChangeExpansion: this.onChangeExpansion,
      };
    }

    if (data) {
      return (
        <SortableTable
          count={data.length}
          sortSetting={sortSetting}
          onChangeSortSetting={onChangeSortSetting}
          columns={this.columns(this.props)}
          rowClass={this.rowClass(this.props)}
          className={this.props.className}
          expandableConfig={expandableConfig}
        />
      );
    }
    return <div>没有结果.</div>;
  }
}
