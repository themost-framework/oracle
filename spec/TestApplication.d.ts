import {DataApplication, DataContext} from "@themost/data";
import {DataAdapterIndexes, DataAdapterTable, DataAdapterView} from "@themost/common";

export declare class TestApplication extends DataApplication {

    async finalize(): Promise<void>;
    async finalizeAsync(): Promise<void>;
    createContext(): DataContext;
    executeInTestTransaction(func: (context: DataContext) => Promise<void>): Promise<void>;
    async tryUpgrade(): Promise<void>;
    async trySetData(): Promise<void>;

}

declare module "@themost/common" {
    export interface DataAdapterBase {
        table(name: string): DataAdapterTable;
        view(name: string): DataAdapterView;
        indexes(name: string): DataAdapterIndexes;
        executeAsync(query: any, values?: any): Promise<any>;
    }
}

declare module "@themost/data" {
    export abstract class DataCacheStrategy {
        async finalize(): Promise<void>;
    }
}