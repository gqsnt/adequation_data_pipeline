// types/pipeline.d.ts
export type Field = { name: string; type: string; nullable: boolean };
export type ArrowLikeSchema =   Field[] ;

export type Source = {
    id: number;
    name: string;
    uri: string;
    config: { delimiter: string; has_header: boolean; encoding: string };
};

export type Dataset = {
    id: number;
    project_id: string;
    name: string; // 'bronze' = source.name ; 'silver' ; or gold name
    layer: 'bronze' | 'silver' | 'gold';
    schema: ArrowLikeSchema;
    primary_key: string[] | null;
    source_id?: number | null; // filled for bronze only
};

export type SilverDataset = {
    id: number;
    schema: ArrowLikeSchema;
    primary_key: string[];
};

export type ExprIR =
    | { col: string }
    | { lit: any }
    | {
    fn_: string;
    args?: ExprIR[];
    to?: string;
    fmt?: string;
    len?: number;
    pred?: ExprIR;
    then?: ExprIR;
    else?: ExprIR;
};

export type MappingColumn = { target: string; expr: ExprIR };

export type Transforms = {
    filters: ExprIR[];
    columns: MappingColumn[];
};

export type MappingCreatePayload = {
    from_dataset_id: number;
    to_dataset_id: number;
    transforms: Transforms;
    dq_rules?: Array<{ column: string; op: string; value?: any }>;
};

export type Pipeline = {
    id: number;
    name: string;
    mapping_silver_id?: number | null;
    mapping_gold_id?: number | null;
};


type Mapping = { id:number; from_dataset_id:number; to_dataset_id:number };
