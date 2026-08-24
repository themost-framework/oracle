import { SqlFormatter, FormatterSettings } from "@themost/query";

export declare interface OracleFormatterSettings extends FormatterSettings {
    /**
     * The character used to quote identifiers. Default is '"'.
     */
    jsonDateFormat?: string;
}

export declare class OracleFormatter extends SqlFormatter {
    settings: OracleFormatterSettings
}