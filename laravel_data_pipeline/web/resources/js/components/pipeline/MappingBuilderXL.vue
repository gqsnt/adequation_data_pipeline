<!-- components/pipeline/MappingBuilderXL.vue -->
<script setup lang="ts">
import { computed, reactive, ref, watchEffect } from 'vue';
import { useForm } from '@inertiajs/vue3';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '../ui/dialog';
import { Button } from '../ui/button';
import { Label } from '../ui/label';
import { Input } from '../ui/input';
import { Textarea } from '../ui/textarea';
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '../ui/select';

type Field = { name: string; type: string; nullable?: boolean };
type ArrowLikeSchema = Field[];

type ColumnConfig = {
    target: string;
    mode: 'identity'|'cast'|'to_date'|'zfill'|'math'|'compare'|'when'|'advanced';
    src?: string | null;
    cast_to?: 'str'|'i64'|'f64'|'bool'|'date'|'datetime';
    date_fmt?: string;
    zfill_len?: number;
    // math/compare
    op?: '+'|'-'|'*'|'/'|'=='|'!='|'>'|'>='|'<'|'<=';
    a_type?: 'col'|'lit';
    a_col?: string|null;
    a_lit?: string;
    b_type?: 'col'|'lit';
    b_col?: string|null;
    b_lit?: string;
    // when
    when_pred_op?: '=='|'!='|'>'|'>='|'<'|'<='|'is_null'|'is_not_null';
    when_left_type?: 'col'|'lit';
    when_left_col?: string|null;
    when_left_lit?: string;
    when_right_type?: 'col'|'lit';
    when_right_col?: string|null;
    when_right_lit?: string;
    when_then_type?: 'col'|'lit';
    when_then_col?: string|null;
    when_then_lit?: string;
    when_else_type?: 'col'|'lit';
    when_else_col?: string|null;
    when_else_lit?: string;
    // advanced
    advanced_json?: string;
};

const props = defineProps<{
    open: boolean;
    onClose: () => void;
    projectId: number|string;
    fromLabel: string;
    toLabel: string;
    fromDatasetId: number|null;
    toDatasetId: number|null;
    fromSchema: ArrowLikeSchema;
    toSchema: ArrowLikeSchema;
}>();

const sourceCols = computed<string[]>(() => (props.fromSchema ?? []).map(f => f.name));
const targetCols = computed<Field[]>(() => props.toSchema ?? []);
const targetNames = computed<string[]>(() => targetCols.value.map(f => f.name));

const cols = ref<ColumnConfig[]>([]);
const filters = ref<Array<{ col: string | null; op: string; value?: string }>>([]);
const dqRules = ref<Array<{ column: string | null; op: string; value?: string }>>([]);

watchEffect(() => {
    if (!props.open) return;
    const srcSet = new Set(sourceCols.value);
    cols.value = targetNames.value.map((t) => ({
        target: t,
        mode: srcSet.has(t) ? 'identity' : 'advanced',
        src: srcSet.has(t) ? t : null,
        cast_to: undefined,
        date_fmt: '%Y-%m-%d',
        zfill_len: 2,
        op: '+',
        a_type: 'col', a_col: srcSet.has(t) ? t : null, a_lit: '',
        b_type: 'lit', b_col: null, b_lit: '0',
        when_pred_op: '==',
        when_left_type: 'col', when_left_col: srcSet.has(t) ? t : null, when_left_lit: '',
        when_right_type: 'lit', when_right_col: null, when_right_lit: '',
        when_then_type: 'col', when_then_col: srcSet.has(t) ? t : null, when_then_lit: '',
        when_else_type: 'lit', when_else_col: null, when_else_lit: 'null',
        advanced_json: '',
    }));
    filters.value = [];
    dqRules.value = [];
});

function litOrCol(type: 'col'|'lit', col: string|null, lit: string) {
    if (type === 'col') return { col: col ?? '' };
    try { return { lit: JSON.parse(lit) } } catch { return { lit } }
}

function buildExprForColumn(c: ColumnConfig): any {
    if (c.mode === 'advanced' && c.advanced_json?.trim()) {
        try { return JSON.parse(c.advanced_json) } catch {/*ignore*/ }
    }
    if (!c.src && !['math','compare','when','advanced'].includes(c.mode)) return { lit: null };
    switch (c.mode) {
        case 'identity': return { col: c.src! };
        case 'cast':     return { fn_: 'cast', args: [{ col: c.src! }], to: c.cast_to ?? 'str' };
        case 'to_date':  return { fn_: 'to_date', args: [{ col: c.src! }], fmt: c.date_fmt ?? '%Y-%m-%d' };
        case 'zfill':    return { fn_: 'zfill', args: [{ col: c.src! }], len: c.zfill_len ?? 2 };
        case 'math':     return { fn_: (c.op ?? '+'), args: [ litOrCol(c.a_type!, c.a_col!, c.a_lit ?? ''), litOrCol(c.b_type!, c.b_col!, c.b_lit ?? '') ] };
        case 'compare':  return { fn_: (c.op ?? '=='), args: [ litOrCol(c.a_type!, c.a_col!, c.a_lit ?? ''), litOrCol(c.b_type!, c.b_col!, c.b_lit ?? '') ] };
        case 'when': {
            if (c.when_pred_op === 'is_null' || c.when_pred_op === 'is_not_null') {
                const a = litOrCol(c.when_left_type!, c.when_left_col!, c.when_left_lit ?? '');
                const pred = { fn_: c.when_pred_op, args: [a] };
                const thenE = litOrCol(c.when_then_type!, c.when_then_col!, c.when_then_lit ?? '');
                const elseE = litOrCol(c.when_else_type!, c.when_else_col!, c.when_else_lit ?? '');
                return { fn_: 'when', pred, then: thenE, else: elseE };
            } else {
                const a = litOrCol(c.when_left_type!, c.when_left_col!, c.when_left_lit ?? '');
                const b = litOrCol(c.when_right_type!, c.when_right_col!, c.when_right_lit ?? '');
                const pred = { fn_: (c.when_pred_op ?? '=='), args: [a, b] };
                const thenE = litOrCol(c.when_then_type!, c.when_then_col!, c.when_then_lit ?? '');
                const elseE = litOrCol(c.when_else_type!, c.when_else_col!, c.when_else_lit ?? '');
                return { fn_: 'when', pred, then: thenE, else: elseE };
            }
        }
        default: return { lit: null };
    }
}

function buildFiltersIR() {
    const out: any[] = [];
    for (const f of filters.value) {
        if (!f.col || !f.op) continue;
        if (f.op === 'is_not_null' || f.op === 'is_null') out.push({ fn_: f.op, args: [{ col: f.col }] });
        else {
            const n = Number(f.value);
            const val: any = Number.isFinite(n) ? { lit: n } : (f?.value?.length ? { lit: f.value } : { lit: null });
            out.push({ fn_: f.op, args: [{ col: f.col }, val] });
        }
    }
    return out;
}

const form = useForm({
    from_dataset_id: null as number | null,
    to_dataset_id: null as number | null,
    transforms: { filters: [] as any[], columns: [] as any[] },
    dq_rules: [] as any[],
});

function submit() {
    if (!props.fromDatasetId || !props.toDatasetId) return;

    form.from_dataset_id = props.fromDatasetId;
    form.to_dataset_id = props.toDatasetId;
    form.transforms = {
        filters: buildFiltersIR(),
        columns: cols.value.map(c => ({ target: c.target, expr: buildExprForColumn(c) })),
    };
    form.dq_rules = dqRules.value.filter(r => r.column && r.op).map(r => {
        const out: any = { column: r.column!, op: r.op };
        if (r.value?.length) { try { out.value = JSON.parse(r.value) } catch { out.value = r.value } }
        return out;
    });

    form.post(`/projects/${props.projectId}/mappings`, { preserveScroll: true, onSuccess: () => props.onClose?.() });
}
</script>

<template>
    <Dialog :open="open" @update:open="onClose">
        <!-- Modal large + zones scrollables -->
        <DialogContent class="w-screen max-w-[1800px] h-[90vh] p-0">
            <div class="flex items-center justify-between px-4 py-3 border-b bg-white sticky top-0 z-10">
                <DialogHeader class="p-0">
                    <DialogTitle>Mapping ({{ fromLabel }} → {{ toLabel }})</DialogTitle>
                </DialogHeader>
                <div class="flex gap-2">
                    <Button variant="secondary" @click="onClose()">Annuler</Button>
                    <Button :disabled="form.processing" @click="submit">Enregistrer</Button>
                </div>
            </div>

            <div class="h-[80vh] grid grid-cols-12 gap-3 p-3 overflow-hidden">
                <!-- Source cols -->
                <section class="col-span-3 rounded border flex flex-col">
                    <div class="p-2 text-sm font-medium">Colonnes source ({{ fromLabel }})</div>
                    <div class="p-2 text-xs overflow-auto">
                        <ul class="space-y-1">
                            <li v-for="c in sourceCols" :key="c" class="truncate">{{ c }}</li>
                        </ul>
                    </div>
                </section>

                <!-- Table de mapping -->
                <section class="col-span-6 rounded border overflow-hidden flex flex-col">
                    <div class="p-2 text-sm font-medium">Cibles ({{ toLabel }})</div>
                    <div class="overflow-auto">
                        <table class="w-full text-xs">
                            <thead class="sticky top-0 bg-white">
                            <tr class="border-b">
                                <th class="p-2 text-left">Target ({{ toLabel }})</th>
                                <th class="p-2 text-left">Source</th>
                                <th class="p-2 text-left">Transform</th>
                                <th class="p-2 text-left">Options</th>
                            </tr>
                            </thead>
                            <tbody>
                            <tr v-for="c in cols" :key="c.target" class="border-b align-top">
                                <td class="p-2 font-medium">{{ c.target }}</td>
                                <td class="p-2">
                                    <select v-model="c.src" class="w-full rounded border px-2 py-1">
                                        <option :value="null">--</option>
                                        <option v-for="sc in sourceCols" :key="sc" :value="sc">{{ sc }}</option>
                                    </select>
                                </td>
                                <td class="p-2">
                                    <Select v-model="c.mode">
                                        <SelectTrigger class="w-44"><SelectValue placeholder="Transform" /></SelectTrigger>
                                        <SelectContent>
                                            <SelectGroup>
                                                <SelectItem value="identity">identity</SelectItem>
                                                <SelectItem value="cast">cast</SelectItem>
                                                <SelectItem value="to_date">to_date</SelectItem>
                                                <SelectItem value="zfill">zfill</SelectItem>
                                                <SelectItem value="math">math</SelectItem>
                                                <SelectItem value="compare">compare</SelectItem>
                                                <SelectItem value="when">when</SelectItem>
                                                <SelectItem value="advanced">advanced (JSON)</SelectItem>
                                            </SelectGroup>
                                        </SelectContent>
                                    </Select>
                                </td>
                                <td class="p-2 space-y-2">
                                    <!-- options selon mode -->
                                    <div v-if="c.mode==='cast'" class="flex items-center gap-2">
                                        <Label class="text-[11px]">to</Label>
                                        <select v-model="c.cast_to" class="rounded border px-2 py-1">
                                            <option value="str">str</option><option value="i64">i64</option>
                                            <option value="f64">f64</option><option value="bool">bool</option>
                                            <option value="date">date</option><option value="datetime">datetime</option>
                                        </select>
                                    </div>

                                    <div v-else-if="c.mode==='to_date'" class="flex items-center gap-2">
                                        <Label class="text-[11px]">fmt</Label>
                                        <Input v-model="c.date_fmt" class="h-7 w-36" placeholder="%Y-%m-%d" />
                                    </div>

                                    <div v-else-if="c.mode==='zfill'" class="flex items-center gap-2">
                                        <Label class="text-[11px]">len</Label>
                                        <Input v-model.number="c.zfill_len" class="h-7 w-20" type="number" min="1" />
                                    </div>

                                    <!-- math / compare -->
                                    <div v-else-if="c.mode==='math' || c.mode==='compare'" class="grid grid-cols-3 gap-2 items-center">
                                        <div>
                                            <Label class="text-[11px]">A</Label>
                                            <div class="flex gap-2">
                                                <select v-model="c.a_type" class="rounded border px-2 py-1">
                                                    <option value="col">col</option><option value="lit">lit</option>
                                                </select>
                                                <select v-if="c.a_type==='col'" v-model="c.a_col" class="rounded border px-2 py-1">
                                                    <option v-for="sc in sourceCols" :key="sc" :value="sc">{{ sc }}</option>
                                                </select>
                                                <Input v-else v-model="c.a_lit" class="h-7" placeholder="ex: 2.5" />
                                            </div>
                                        </div>
                                        <div>
                                            <Label class="text-[11px]">op</Label>
                                            <select v-model="c.op" class="rounded border px-2 py-1">
                                                <option value="+">+</option><option value="-">-</option>
                                                <option value="*">*</option><option value="/">/</option>
                                                <option value="==">==</option><option value="!=">!=</option>
                                                <option value=">">&gt;</option><option value=">=">&ge;</option>
                                                <option value="<">&lt;</option><option value="<=">&le;</option>
                                            </select>
                                        </div>
                                        <div>
                                            <Label class="text-[11px]">B</Label>
                                            <div class="flex gap-2">
                                                <select v-model="c.b_type" class="rounded border px-2 py-1">
                                                    <option value="col">col</option><option value="lit">lit</option>
                                                </select>
                                                <select v-if="c.b_type==='col'" v-model="c.b_col" class="rounded border px-2 py-1">
                                                    <option v-for="sc in sourceCols" :key="sc" :value="sc">{{ sc }}</option>
                                                </select>
                                                <Input v-else v-model="c.b_lit" class="h-7" placeholder="ex: 1000" />
                                            </div>
                                        </div>
                                    </div>

                                    <!-- when -->
                                    <div v-else-if="c.mode==='when'" class="grid grid-cols-2 gap-2">
                                        <div class="space-y-1">
                                            <Label class="text-[11px]">pred op</Label>
                                            <select v-model="c.when_pred_op" class="rounded border px-2 py-1">
                                                <option value="==">==</option><option value="!=">!=</option>
                                                <option value=">">&gt;</option><option value=">=">&ge;</option>
                                                <option value="<">&lt;</option><option value="<=">&le;</option>
                                                <option value="is_null">is_null</option><option value="is_not_null">is_not_null</option>
                                            </select>
                                        </div>
                                        <div class="space-y-1">
                                            <Label class="text-[11px]">left</Label>
                                            <div class="flex gap-2">
                                                <select v-model="c.when_left_type" class="rounded border px-2 py-1">
                                                    <option value="col">col</option><option value="lit">lit</option>
                                                </select>
                                                <select v-if="c.when_left_type==='col'" v-model="c.when_left_col" class="rounded border px-2 py-1">
                                                    <option v-for="sc in sourceCols" :key="sc" :value="sc">{{ sc }}</option>
                                                </select>
                                                <Input v-else v-model="c.when_left_lit" class="h-7" placeholder='"N"' />
                                            </div>
                                        </div>
                                        <div class="space-y-1">
                                            <Label class="text-[11px]">right (if applicable)</Label>
                                            <div class="flex gap-2">
                                                <select v-model="c.when_right_type" class="rounded border px-2 py-1">
                                                    <option value="col">col</option><option value="lit">lit</option>
                                                </select>
                                                <select v-if="c.when_right_type==='col'" v-model="c.when_right_col" class="rounded border px-2 py-1">
                                                    <option v-for="sc in sourceCols" :key="sc" :value="sc">{{ sc }}</option>
                                                </select>
                                                <Input v-else v-model="c.when_right_lit" class="h-7" placeholder="ex: 0" />
                                            </div>
                                        </div>
                                        <div class="space-y-1">
                                            <Label class="text-[11px]">then</Label>
                                            <div class="flex gap-2">
                                                <select v-model="c.when_then_type" class="rounded border px-2 py-1">
                                                    <option value="col">col</option><option value="lit">lit</option>
                                                </select>
                                                <select v-if="c.when_then_type==='col'" v-model="c.when_then_col" class="rounded border px-2 py-1">
                                                    <option v-for="sc in sourceCols" :key="sc" :value="sc">{{ sc }}</option>
                                                </select>
                                                <Input v-else v-model="c.when_then_lit" class="h-7" placeholder='"OK"' />
                                            </div>
                                        </div>
                                        <div class="space-y-1">
                                            <Label class="text-[11px]">else</Label>
                                            <div class="flex gap-2">
                                                <select v-model="c.when_else_type" class="rounded border px-2 py-1">
                                                    <option value="col">col</option><option value="lit">lit</option>
                                                </select>
                                                <select v-if="c.when_else_type==='col'" v-model="c.when_else_col" class="rounded border px-2 py-1">
                                                    <option v-for="sc in sourceCols" :key="sc" :value="sc">{{ sc }}</option>
                                                </select>
                                                <Input v-else v-model="c.when_else_lit" class="h-7" placeholder="null" />
                                            </div>
                                        </div>
                                    </div>

                                    <!-- advanced -->
                                    <div v-else-if="c.mode==='advanced'">
                                        <Textarea v-model="c.advanced_json" class="min-h-[80px]" placeholder='{"fn_":"when","pred":{...},"then":{...},"else":{...}}' />
                                    </div>

                                    <div v-else class="text-muted-foreground">—</div>
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </section>

                <!-- Filtres + DQ -->
                <section class="col-span-3 space-y-4 overflow-auto">
                    <div class="rounded border p-2">
                        <div class="text-sm font-medium">Filters ({{ fromLabel }})</div>
                        <div class="mt-2 space-y-2">
                            <div v-for="(f, idx) in filters" :key="idx" class="grid grid-cols-4 items-center gap-2">
                                <div class="col-span-2">
                                    <Label class="text-xs">Column</Label>
                                    <select v-model="f.col" class="w-full rounded border px-2 py-1 text-xs">
                                        <option :value="null">--</option>
                                        <option v-for="c in sourceCols" :key="c" :value="c">{{ c }}</option>
                                    </select>
                                </div>
                                <div>
                                    <Label class="text-xs">Op</Label>
                                    <select v-model="f.op" class="w-full rounded border px-2 py-1 text-xs">
                                        <option value=">">&gt;</option><option value=">=">&ge;</option>
                                        <option value="<">&lt;</option><option value="<=">&le;</option>
                                        <option value="==">==</option><option value="!=">!=</option>
                                        <option value="is_not_null">is_not_null</option><option value="is_null">is_null</option>
                                    </select>
                                </div>
                                <div>
                                    <Label class="text-xs">Value</Label>
                                    <Input v-model="f.value" :disabled="f.op==='is_not_null'||f.op==='is_null'" class="h-7" placeholder="ex: 0" />
                                </div>
                                <div class="mt-5"><Button size="sm" variant="outline" @click="filters.splice(idx,1)">Remove</Button></div>
                            </div>
                            <div><Button size="sm" variant="secondary" @click="filters.push({ col: null, op: 'is_not_null' })">+ Add filter</Button></div>
                        </div>
                    </div>

                    <div class="rounded border p-2">
                        <div class="text-sm font-medium">DQ Rules ({{ toLabel }})</div>
                        <div class="mt-2 space-y-2">
                            <div v-for="(r, idx) in dqRules" :key="idx" class="grid grid-cols-4 items-center gap-2">
                                <div class="col-span-2">
                                    <Label class="text-xs">Column</Label>
                                    <select v-model="r.column" class="w-full rounded border px-2 py-1 text-xs">
                                        <option :value="null">--</option>
                                        <option v-for="c in targetNames" :key="c" :value="c">{{ c }}</option>
                                    </select>
                                </div>
                                <div>
                                    <Label class="text-xs">Op</Label>
                                    <select v-model="r.op" class="w-full rounded border px-2 py-1 text-xs">
                                        <option value=">">&gt;</option><option value=">=">&ge;</option>
                                        <option value="<">&lt;</option><option value="<=">&le;</option>
                                        <option value="==">==</option><option value="!=">!=</option>
                                        <option value="is_not_null">is_not_null</option><option value="is_null">is_null</option>
                                    </select>
                                </div>
                                <div>
                                    <Label class="text-xs">Value (JSON/str)</Label>
                                    <Input v-model="r.value" class="h-7" placeholder='ex: 0' />
                                </div>
                                <div class="mt-5"><Button size="sm" variant="outline" @click="dqRules.splice(idx,1)">Remove</Button></div>
                            </div>
                            <div><Button size="sm" variant="secondary" @click="dqRules.push({ column: null, op: 'is_not_null' })">+ Add DQ</Button></div>
                        </div>
                    </div>
                </section>
            </div>

            <DialogFooter class="border-t p-3 sticky bottom-0 bg-white">
                <div class="text-xs text-muted-foreground">Les paramètres sont traduits en IR JSON compatible worker (Polars).</div>
            </DialogFooter>
        </DialogContent>
    </Dialog>
</template>
