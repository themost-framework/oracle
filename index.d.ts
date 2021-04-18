
// Copyright (c) 2017-2021, THEMOST LP. All rights reserved.
import { SqlFormatter } from "@themost/query";

export declare interface OracleAdapterTable {
    create(fields: Array<any>, callback: (err: Error) => void): void;
    createAsync(fields: Array<any>): Promise<void>;
    add(fields: Array<any>, callback: (err: Error) => void): void;
    addAsync(fields: Array<any>): Promise<void>;
    change(fields: Array<any>, callback: (err: Error) => void): void;
    changeAsync(fields: Array<any>): Promise<void>;
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    version(callback: (err: Error, result: string) => void): void;
    versionAsync(): Promise<string>;
    columns(callback: (err: Error, result: Array<any>) => void): void;
    columnsAsync(): Promise<Array<any>>;
}

// export declare interface OracleAdapterIndex {
//     name: string;
//     columns: Array<string>;
// }

// export declare interface OracleAdapterIndexes {
//     create(name: string, columns: Array<string>, callback: (err: Error, res?: number) => void): void;
//     createAsync(name: string, columns: Array<string>): Promise<number>;
//     drop(name: string, callback: (err: Error, res?: number) => void): void;
//     dropAsync(name: string): Promise<number>;
//     list(callback: (err: Error, res: Array<OracleAdapterIndex>) => void): void;
//     listAsync(): Promise<Array<OracleAdapterIndex>>;
// }

export declare interface OracleAdapterView {
    create(query: any, callback: (err: Error) => void): void;
    createAsync(query: any): Promise<void>;
    exists(callback: (err: Error, result: boolean) => void): void;
    existsAsync(): Promise<boolean>;
    drop(callback: (err: Error) => void): void;
    dropAsync(): Promise<void>;
}

// export declare interface OracleAdapterDatabase {
//     exists(callback: (err: Error, result: boolean) => void): void;
//     existsAsync(): Promise<boolean>;
//     create(callback: (err: Error) => void): void;
//     createAsync(): Promise<void>;
// }

export declare interface OracleAdapterMigration {
    add: Array<any>;
    change?: Array<any>;
    appliesTo: string;
    version: string;
}

export declare class OracleAdapter {
    static formatType(field: any): string;
    formatType(field: any): string;
    open(callback: (err: Error) => void): void;
    close(callback: (err: Error) => void): void;
    openAsync(): Promise<void>;
    closeAsync(): Promise<void>;
    prepare(query: any, values?: Array<any>): any;
    createView(name: string, query: any, callback: (err: Error) => void): void;
    executeInTransaction(func: any, callback: (err: Error) => void): void;
    executeInTransactionAsync(func: Promise<any>): Promise<any>;
    migrate(obj: OracleAdapterMigration, callback: (err: Error) => void): void;
    selectIdentity(entity: string, attribute: string, callback: (err: Error, value: any) => void): void;
    execute(query: any, values: any, callback: (err: Error, value: any) => void): void;
    executeAsync(query: any, values: any): Promise<any>;
    executeAsync<T>(query: any, values: any): Promise<Array<T>>;
    table(name: string): OracleAdapterTable;
    view(name: string): OracleAdapterView;
    // indexes(name: string): OracleAdapterIndexes;
    // database(name: string): OracleAdapterDatabase;
}

export declare class OracleFormatter extends SqlFormatter {
}
