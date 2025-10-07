<script setup lang="ts">
import { Head, router, useForm } from '@inertiajs/vue3'
import { computed, reactive, ref, watch } from 'vue'

import AppLayout from '@/layouts/AppLayout.vue'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

import SilverSchemaEditor from '@/components/pipeline/SilverSchemaEditor.vue'
import MappingBuilderXL from '@/components/pipeline/MappingBuilderXL.vue';
import { Mapping } from '@/types/pipeline';

type Field = { name: string; type: string; nullable: boolean }
type Source = { id: number; name: string; uri: string; config: {Csv:{ delimiter: string; has_header: boolean; encoding: string }} }
type Dataset = { id: number; project_id: string; name: string; layer: 'bronze'|'silver'|'gold'; schema: Field[]; primary_key: string[] | null; source_id?: number | null }
type SilverDataset = { id: number; schema: Field[]; primary_key: string[] }

type ProjectProp = {
    id: number | string
    namespace: string
    warehouse_uri: string
    sources: Source[]
    datasets: Dataset[]
    mappings: any[]
    pipelines: any[]
    runs: any[]
    silver?: SilverDataset | null
}

const props = defineProps<{ project: ProjectProp }>()

/* ---------- selections & derived ---------- */
const selectedSourceId = ref<number | null>(props.project.sources?.[0]?.id ?? null)
const selectedSource = computed(() => (props.project.sources ?? []).find(s => s.id === selectedSourceId.value) || null)

const datasets = computed<Dataset[]>(() => (props.project as any).datasets ?? []);
const silver = ref<SilverDataset | null>(props.project.silver ?? null);

const goldList = computed<Dataset[]>(() => datasets.value.filter(d => d.layer === 'gold'));
const selectedGoldId = ref<number | null>(goldList.value[0]?.id ?? null);
const selectedGold = computed<Dataset | null>(() => goldList.value.find(g => g.id === selectedGoldId.value) ?? null);
const newGoldOpen = ref(false);
const editGoldOpen = ref(false);

const newGoldForm = useForm({
    name: '',
    schema: [] as Field[],
    primary_key: [] as string[],
});
const seedModeGold = ref<'overwrite'|'append'>('overwrite');

function seedGoldFromSilver() {
    const src = silver.value?.schema ?? [];
    if (seedModeGold.value === 'overwrite') {
        newGoldForm.schema = src.map(f => ({ ...f })) ;
        newGoldForm.primary_key = (silver.value?.primary_key ?? []).slice();
    } else {
        const base = newGoldForm.schema ?? [];
        const have = new Set(base.map(f => f.name));
        const merged = [...base];
        for (const f of src) if (!have.has(f.name)) merged.push({ ...f });
        newGoldForm.schema = merged;
        if (!newGoldForm.primary_key?.length) newGoldForm.primary_key = (silver.value?.primary_key ?? []).slice();
    }
}

function createGold(close: () => void) {
    newGoldForm.post(`/projects/${props.project.id}/gold`, {
        preserveScroll: true,
        onSuccess: () => { newGoldForm.reset(); close(); },
    });
}

function saveGoldSchema(payload: { fields:any[]; primary_key:string[] }) {
    if (!selectedGold.value) return;
    router.put(`/projects/${props.project.id}/gold/${selectedGold.value.id}`, {
        schema:payload.fields ,
        primary_key: payload.primary_key,
    }, { preserveScroll: true, onSuccess: () => { /* rechargé par Inertia */ }});
}

function deleteGold() {
    if (!selectedGold.value) return;
    if (!confirm('Supprimer ce dataset Gold ?')) return;
    router.delete(`/projects/${props.project.id}/gold/${selectedGold.value.id}`, { preserveScroll: true });
}
const mapBS_Open = ref(false); // Bronze -> Silver
const mapSG_Open = ref(false); // Silver -> Gold

const bronzeForSelected = computed<Dataset | null>(() => {
    const s = selectedSource.value;
    if (!s) return null;
    for (const ds of datasets.value) if (ds.layer==='bronze' && ds.source_id === s.id) return ds;
    return null;
});
const bronzeBySourceId = computed<Record<number, Dataset | undefined>>(() => {
    const out: Record<number, Dataset | undefined> = {}
    for (const ds of datasets.value) if (ds.layer === 'bronze' && ds.source_id) out[ds.source_id] = ds
    return out
})


watch(() => props.project.sources, (srcs) => {
    if (!srcs?.length) selectedSource.value = null
    else if (!selectedSource.value) selectedSource.value = srcs[0]
    else {
        const match = srcs.find(s => s.id === selectedSource.value?.id)
        if (!match) selectedSource.value = srcs[0]
        else selectedSource.value = match
    }
},{ immediate: true })
watch(() => props.project.silver, (s) => { silver.value = s ?? null }, { immediate: true })

/* ---------- Sources CRUD + infer schema ---------- */
const newSourceOpen = ref(false)
const newSrc = useForm({ name:'', uri:'', config:{Csv:{ delimiter:',', has_header:true, encoding:'utf-8' } }})
function createSource(close: () => void) {
    router.post(`/projects/${props.project.id}/sources`, newSrc, { preserveScroll:true, onSuccess: () => { newSrc.reset(); close() } })
}
const editSourceOpen = ref(false)
const editSrc = useForm({ name:'', uri:'', config:{ Csv:{delimiter:',', has_header:true, encoding:'utf-8' }} })
watch(selectedSource, (s) => {
    if (!s) return
    editSrc.name = s.name
    editSrc.uri = s.uri
    editSrc.config = { Csv: {
            delimiter: s.config?.Csv.delimiter ?? ',',
            has_header: s.config?.Csv.has_header ?? true,
            encoding: s.config?.Csv.encoding ?? 'utf-8'
        }}
}, { immediate: true })

function handleHasHeaderEditChange(payload:boolean){
    editSrc.config.Csv.has_header = payload
}

function handleHasHeaderNewChange(value: boolean) {
    newSrc.config.Csv.has_header = value
}

function updateSource(close: () => void) {
    if (!selectedSource.value) return
    router.put(`/projects/${props.project.id}/sources/${selectedSource.value.id}`, editSrc, { preserveScroll:true, onSuccess: () => close() })
}
function deleteSource() {
    if (!selectedSource.value) return
    if (!confirm('Delete this source? This cannot be undone.')) return
    router.delete(`/projects/${props.project.id}/sources/${selectedSource.value.id}`, { preserveScroll:true })
}
function syncSchema() {
    if (!selectedSource.value) return
    router.post(`/projects/${props.project.id}/sources/${selectedSource.value.id}/infer_schema`, {}, { preserveScroll:true })
}

/* ---------- Schema modal (improved) ---------- */
const schemaModal = reactive<{ open: boolean; sourceId: number | null; schema: Field[] | null; error: string | null }>({ open:false, sourceId:null, schema:null, error:null })
function showSchema() {
    if (!selectedSource.value) return
    const b = bronzeForSelected.value
    schemaModal.sourceId = selectedSource.value.id
    if (b?.schema?.length) { schemaModal.schema = b.schema; schemaModal.error = null }
    else { schemaModal.schema = null; schemaModal.error = 'Aucun schéma bronze : exécutez "Sync schema" d’abord.' }
    schemaModal.open = true
}

/* ---------- Silver editor ---------- */
const silverKey = ref(0)
function saveSilver(payload: { fields: Field[]; primary_key: string[] }) {
    return router.put(
        `/projects/${props.project.id}/silver`,
        { schema: payload.fields, primary_key: payload.primary_key },
        { preserveScroll: true, onSuccess: () => { silver.value = { id: silver.value?.id ?? 0, schema: payload.fields , primary_key: payload.primary_key }; silverKey.value++ } }
    )
}
function seedFromSelectedSource() {
    if (!selectedSource.value) return
    router.post(`/projects/${props.project.id}/silver/seed-from-source/${selectedSource.value.id}`, {}, { preserveScroll:true })
}

const mappings = computed<Mapping[]>(() => (props.project.mappings ?? []) as any);

const datasetsById = computed<Record<number, any>>(() => {
    const map: Record<number, any> = {};
    for (const d of (props.project.datasets ?? [])) map[d.id] = d;
    return map;
});

const mappingBS = computed(() =>
    mappings.value
        .map(m => ({ m, from: datasetsById.value[m.from_dataset_id], to: datasetsById.value[m.to_dataset_id] }))
        .filter(x => x.from?.layer === 'bronze' && x.to?.layer === 'silver')
);

const mappingSG = computed(() =>
    mappings.value
        .map(m => ({ m, from: datasetsById.value[m.from_dataset_id], to: datasetsById.value[m.to_dataset_id] }))
        .filter(x => x.from?.layer === 'silver' && x.to?.layer === 'gold')
);

/* Create pipeline */
const pipeCreateOpen = ref(false);
const pipeForm = useForm<{ name:string; mapping_silver_id:number|null; mapping_gold_id:number|null }>({
    name: '',
    mapping_silver_id: mappingBS.value[0]?.m.id ?? null,
    mapping_gold_id: mappingSG.value[0]?.m.id ?? null,
});
function createPipeline(close: () => void) {
    pipeForm.post(`/projects/${props.project.id}/pipelines`, {
        preserveScroll: true,
        onSuccess: () => { pipeForm.reset(); close(); },
    });
}

/* Edit pipeline */
const pipeEditOpen = ref(false);
const currentPipe = ref<any|null>(null);
const pipeEditForm = useForm<{ name?:string; mapping_silver_id:number|null; mapping_gold_id:number|null }>({
    name: '',
    mapping_silver_id: null,
    mapping_gold_id: null,
});

function openEditPipeline(p:any) {
    currentPipe.value = p;
    pipeEditForm.name = p.name;
    pipeEditForm.mapping_silver_id = p.mapping_silver_id ?? null;
    pipeEditForm.mapping_gold_id = p.mapping_gold_id ?? null;
    pipeEditOpen.value = true;
}

function savePipeline(close: () => void) {
    if (!currentPipe.value) return;
    pipeEditForm.put(`/projects/${props.project.id}/pipelines/${currentPipe.value.id}`, {
        preserveScroll: true,
        onSuccess: () => close(),
    });
}

function deletePipeline(p:any) {
    if (!confirm(`Delete pipeline "${p.name}" ?`)) return;
    router.delete(`/projects/${props.project.id}/pipelines/${p.id}`, { preserveScroll: true });
}




/* ---------- Runs ---------- */
const showNewRun = ref(false)
const runForm = useForm<{ pipeline_id: number | null }>({ pipeline_id: (props.project.pipelines ?? [])[0]?.id ?? null })
function newRun(close: () => void) {
    router.post(`/projects/${props.project.id}/runs`, runForm, { preserveScroll: true, onSuccess: () => close() })
}

/* ---------- Errors modal ---------- */
const errorModal = ref<{ open: boolean; run: any | null }>({ open: false, run: null })
function openErrors(run: any) { errorModal.value = { open: true, run } }
</script>

<template>
    <Head :title="`Projet ${project.namespace}`" />
    <AppLayout :breadcrumbs="[{title:'Projects', href:'/projects'}, {title: project.namespace, href:`/projects/${project.id}`}]">
        <div class="space-y-6 p-4">
            <!-- 1) SOURCES -->
            <Card>
                <CardHeader class="flex items-center justify-between">
                    <CardTitle>Sources</CardTitle>
                    <Dialog v-model:open="newSourceOpen">
                        <DialogTrigger as-child><Button size="sm">New Source</Button></DialogTrigger>
                        <DialogContent class="sm:max-w-lg">
                            <DialogHeader><DialogTitle>Create Source</DialogTitle></DialogHeader>
                            <div class="grid gap-3">
                                <div class="space-y-1"><Label>Name</Label><Input v-model="newSrc.name" placeholder="dvf_2020" /></div>
                                <div class="space-y-1"><Label>URI</Label><Input v-model="newSrc.uri" placeholder="file:///samples/dvf_2020.csv" /></div>
                                <div class="grid grid-cols-3 gap-3">
                                    <div class="space-y-1"><Label>Delimiter</Label><Input v-model="newSrc.config.Csv.delimiter" placeholder="," /></div>
                                    <div class="space-y-1">
                                        <Label>Header</Label>
                                        <div class="flex items-center gap-2 mt-2"><Switch :model-value="newSrc.config.Csv.has_header" @update:model-value="handleHasHeaderNewChange" /><span class="text-sm">has header</span></div>
                                    </div>
                                    <div class="space-y-1"><Label>Encoding</Label><Input v-model="newSrc.config.Csv.encoding" placeholder="utf-8" /></div>
                                </div>
                            </div>
                            <DialogFooter>
                                <Button variant="secondary" @click="newSrc.reset()">Reset</Button>
                                <Button @click="createSource(() => (newSourceOpen=false))">Create</Button>
                            </DialogFooter>
                        </DialogContent>
                    </Dialog>
                </CardHeader>
                <CardContent>
                    <div v-if="project.sources.length === 0" class="text-sm text-muted-foreground">Aucune source. Créez-en une pour commencer.</div>
                    <div v-else class="flex flex-wrap items-center gap-3">
                        <label class="text-sm">Source</label>
                        <Select v-model="selectedSourceId" >
                            <SelectTrigger class="rounded border px-2 py-1 text-sm">
                                <SelectValue placeholder="Select a source" />
                            </SelectTrigger>
                            <SelectContent>
                            <SelectItem  v-for="s in project.sources" :key="s.id" :value="s.id">{{ s.name }}</SelectItem >
                            </SelectContent>
                        </Select>
                        <Button size="sm" variant="secondary" @click="syncSchema">Sync schema</Button>
                        <Button size="sm" @click="showSchema">Show schema</Button>
                        <Dialog v-model:open="editSourceOpen">
                            <DialogTrigger as-child><Button size="sm" variant="outline">Edit source</Button></DialogTrigger>
                            <DialogContent class="sm:max-w-xl w-xl">
                                <DialogHeader><DialogTitle>Edit Source</DialogTitle></DialogHeader>
                                <div class="grid gap-3">
                                    <div class="space-y-1"><Label>Name</Label><Input v-model="editSrc.name" /></div>
                                    <div class="space-y-1"><Label>URI</Label><Input v-model="editSrc.uri" /></div>
                                    <div class="grid grid-cols-3 gap-3">
                                        <div class="space-y-1"><Label>Delimiter</Label><Input v-model="editSrc.config.Csv.delimiter" /></div>
                                        <div class="space-y-1">
                                            <Label>Header</Label>
                                            <div class="mt-2 flex items-center gap-2"><Switch :model-value="editSrc.config.Csv.has_header" @update:model-value="handleHasHeaderEditChange"  /><span class="text-sm">has header</span></div>
                                        </div>
                                        <div class="space-y-1"><Label>Encoding</Label><Input v-model="editSrc.config.Csv.encoding" /></div>
                                    </div>
                                </div>
                                <DialogFooter>
                                    <Button variant="destructive" class="mr-auto" @click="deleteSource">Delete</Button>
                                    <Button @click="updateSource(() => (editSourceOpen=false))">Save</Button>
                                </DialogFooter>
                            </DialogContent>
                        </Dialog>
                    </div>
                </CardContent>
            </Card>

            <!-- 2) SILVER -->
            <Card>
                <CardHeader class="flex items-center justify-between">
                    <CardTitle>Silver (target schema)</CardTitle>
                    <div class="flex items-center gap-3">
                        <span class="text-sm text-muted-foreground">Seed from selected Source (server-side)</span>
                        <Button size="sm" :disabled="!selectedSource" @click="seedFromSelectedSource">Apply</Button>
                    </div>
                </CardHeader>
                <CardContent>
                    <div v-if="!silver" class="text-sm text-muted-foreground">Aucun schéma Silver. Seed depuis la source ou définissez-le manuellement.</div>
                    <SilverSchemaEditor
                        :key="silverKey"
                        :initial="silver?.schema ?? []"
                        :initial-primary-key="silver?.primary_key ?? []"
                        :onSave="saveSilver"
                    />
                </CardContent>
            </Card>

            <!-- === GOLD === -->
            <Card>
                <CardHeader class="flex items-center justify-between">
                    <CardTitle>Gold (N datasets)</CardTitle>
                    <Dialog v-model:open="newGoldOpen">
                        <DialogTrigger as-child><Button size="sm">New Gold</Button></DialogTrigger>
                        <DialogContent class="sm:max-w-3xl">
                            <DialogHeader><DialogTitle>Create Gold dataset</DialogTitle></DialogHeader>

                            <div class="grid gap-4">
                                <div class="space-y-1">
                                    <Label>Name</Label>
                                    <Input v-model="newGoldForm.name" placeholder="ex: gold_tx" />
                                </div>

                                <div class="flex items-center gap-3">
                                    <span class="text-sm text-muted-foreground">Seed from Silver:</span>
                                    <Select v-model="seedModeGold">
                                        <SelectTrigger class="rounded border px-2 py-1 text-sm">
                                            <SelectValue placeholder="Select an option" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem  value="overwrite">Overwrite</SelectItem >
                                            <SelectItem  value="append">Append</SelectItem >
                                        </SelectContent>
                                    </Select>
                                    <Button size="sm" :disabled="!silver" @click="seedGoldFromSilver">Apply</Button>
                                </div>

                                <SilverSchemaEditor
                                    :title="'Schéma Gold'"
                                    :initial="newGoldForm.schema"
                                    :initial-primary-key="newGoldForm.primary_key"
                                    :onSave="async ({fields, primary_key}) => { newGoldForm.schema; newGoldForm.primary_key=primary_key; }"
                                />
                            </div>

                            <DialogFooter>
                                <Button variant="secondary" @click="newGoldForm.reset()">Reset</Button>
                                <Button :disabled="newGoldForm.processing || !newGoldForm.name" @click="createGold(() => (newGoldOpen=false))">Create</Button>
                            </DialogFooter>
                        </DialogContent>
                    </Dialog>
                </CardHeader>

                <CardContent>
                    <div v-if="goldList.length===0" class="text-sm text-muted-foreground">
                        Aucun dataset Gold. Créez-en un pour commencer.
                    </div>

                    <div v-else class="space-y-4">
                        <div class="flex items-center gap-3">
                            <Label class="text-sm">Gold</Label>
                            <Select v-model="selectedGoldId" >
                                <SelectTrigger class="rounded border px-2 py-1 text-sm">
                                    <SelectValue placeholder="Select a gold" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem  v-for="g in goldList" :key="g.id" :value="g.id">{{ g.name }}</SelectItem >
                                </SelectContent>
                            </Select>

                            <Dialog v-model:open="editGoldOpen">
                                <DialogTrigger as-child><Button size="sm" variant="outline">Edit</Button></DialogTrigger>
                                <DialogContent class="sm:max-w-5xl">
                                    <DialogHeader><DialogTitle>Edit Gold schema</DialogTitle></DialogHeader>
                                    <SilverSchemaEditor
                                        :title="'Schéma Gold'"
                                        :initial="selectedGold?.schema ?? []"
                                        :initial-primary-key="selectedGold?.primary_key ?? []"
                                        :onSave="saveGoldSchema"
                                    />
                                    <DialogFooter>
                                        <Button variant="destructive" class="mr-auto" @click="deleteGold">Delete</Button>
                                        <Button @click="() => (editGoldOpen=false)">Close</Button>
                                    </DialogFooter>
                                </DialogContent>
                            </Dialog>
                        </div>
                    </div>
                </CardContent>
            </Card>

            <!-- === MAPPINGS (Bronze→Silver & Silver→Gold) === -->
            <Card>
                <CardHeader class="flex items-center justify-between">
                    <div>
                        <CardTitle>Mappings</CardTitle>
                        <div class="text-sm text-muted-foreground">Créez/éditez les mappings Bronze→Silver et Silver→Gold.</div>
                    </div>
                    <div class="flex gap-2">
                        <Button :disabled="!silver || !selectedSource || !bronzeForSelected" @click="mapBS_Open = true">Bronze → Silver</Button>
                        <Button :disabled="!silver || !selectedGold" @click="mapSG_Open = true">Silver → Gold</Button>
                    </div>
                </CardHeader>

                <CardContent>
                    <div v-if="project.mappings.length === 0" class="text-sm text-muted-foreground">Aucun mapping pour l’instant.</div>
                    <ul v-else class="text-sm list-disc ml-5 space-y-1">
                        <li v-for="m in project.mappings" :key="m.id">
                            {{ m.from_dataset_id }} → {{ m.to_dataset_id }}
                        </li>
                    </ul>
                </CardContent>

                <!-- Dialogs de mapping -->
                <MappingBuilderXL
                    :open="mapBS_Open" :onClose="() => (mapBS_Open=false)"
                    :project-id="project.id"
                    from-label="bronze" to-label="silver"
                    :from-dataset-id="bronzeForSelected?.id ?? null"
                    :to-dataset-id="silver?.id ?? null"
                    :from-schema="bronzeForSelected?.schema ??  []"
                    :to-schema="silver?.schema ?? []"
                />
                <MappingBuilderXL
                    :open="mapSG_Open" :onClose="() => (mapSG_Open=false)"
                    :project-id="project.id"
                    from-label="silver" to-label="gold"
                    :from-dataset-id="silver?.id ?? null"
                    :to-dataset-id="selectedGold?.id ?? null"
                    :from-schema="silver?.schema ?? [] "
                    :to-schema="selectedGold?.schema ??  []"
                />
            </Card>


            <!-- === PIPELINES === -->
            <Card>
                <CardHeader class="flex items-center justify-between">
                    <CardTitle>Pipelines</CardTitle>
                    <Dialog v-model:open="pipeCreateOpen">
                        <DialogTrigger as-child><Button size="sm">New Pipeline</Button></DialogTrigger>
                        <DialogContent class="sm:max-w-2xl">
                            <DialogHeader><DialogTitle>Create Pipeline</DialogTitle></DialogHeader>

                            <div class="grid gap-4">
                                <div class="space-y-1">
                                    <Label>Name</Label>
                                    <Input v-model="pipeForm.name" placeholder="ex: bronze_to_silver_to_gold" />
                                </div>
                                <div class="space-y-1">
                                    <Label>Mapping (Bronze → Silver)</Label>
                                    <Select v-model="pipeForm.mapping_silver_id" >
                                        <SelectTrigger class="w-full rounded border px-2 py-1">
                                            <SelectValue placeholder="— none —" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem  v-for="x in mappingBS" :key="x.m.id" :value="x.m.id">
                                                #{{ x.m.id }} · {{ datasetsById[x.m.from_dataset_id]?.name }} → {{ datasetsById[x.m.to_dataset_id]?.name }}
                                            </SelectItem >
                                        </SelectContent>
                                    </Select>
                                    <p class="text-xs text-muted-foreground">Doit être un mapping Bronze → Silver (même projet).</p>
                                </div>
                                <div class="space-y-1">
                                    <Label>Mapping (Silver → Gold)</Label>
                                    <Select v-model="pipeForm.mapping_gold_id" >
                                        <SelectTrigger class="w-full rounded border px-2 py-1">
                                            <SelectValue placeholder="— none —" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem  v-for="x in mappingSG" :key="x.m.id" :value="x.m.id">
                                                #{{ x.m.id }} · {{ datasetsById[x.m.from_dataset_id]?.name }} → {{ datasetsById[x.m.to_dataset_id]?.name }}
                                            </SelectItem >
                                        </SelectContent>
                                    </Select>
                                    <p class="text-xs text-muted-foreground">Doit être un mapping Silver → Gold (même projet).</p>
                                </div>
                            </div>

                            <DialogFooter>
                                <Button variant="secondary" @click="pipeForm.reset()">Reset</Button>
                                <Button :disabled="pipeForm.processing || !pipeForm.name" @click="createPipeline(() => (pipeCreateOpen=false))">Create</Button>
                            </DialogFooter>
                        </DialogContent>
                    </Dialog>
                </CardHeader>

                <CardContent>
                    <div v-if="project.pipelines.length===0" class="text-sm text-muted-foreground">Aucun pipeline.</div>

                    <div v-else class="overflow-x-auto">
                        <table class="w-full text-sm">
                            <thead class="text-left">
                            <tr class="border-b">
                                <th class="py-2 pr-2">Name</th>
                                <th class="py-2 pr-2">Bronze → Silver</th>
                                <th class="py-2 pr-2">Silver → Gold</th>
                                <th class="py-2 pr-2">Actions</th>
                            </tr>
                            </thead>
                            <tbody>
                            <tr v-for="p in project.pipelines" :key="p.id" class="border-b">
                                <td class="py-2 pr-2">{{ p.name }}</td>
                                <td class="py-2 pr-2">
                                    <span v-if="p.mapping_silver_id">#{{ p.mapping_silver_id }}</span>
                                    <span v-else class="text-muted-foreground">—</span>
                                </td>
                                <td class="py-2 pr-2">
                                    <span v-if="p.mapping_gold_id">#{{ p.mapping_gold_id }}</span>
                                    <span v-else class="text-muted-foreground">—</span>
                                </td>
                                <td class="py-2 pr-2">
                                    <div class="flex gap-2">
                                        <Button size="sm" variant="outline" @click="openEditPipeline(p)">Edit</Button>
                                        <Button size="sm" variant="destructive" @click="deletePipeline(p)">Delete</Button>
                                    </div>
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>

                    <!-- Edit dialog -->
                    <Dialog v-model:open="pipeEditOpen">
                        <DialogContent class="sm:max-w-2xl">
                            <DialogHeader><DialogTitle>Edit Pipeline</DialogTitle></DialogHeader>

                            <div class="grid gap-4">
                                <div class="space-y-1">
                                    <Label>Name</Label>
                                    <Input v-model="pipeEditForm.name" />
                                </div>
                                <div class="space-y-1">
                                    <Label>Mapping (Bronze → Silver)</Label>
                                    <Select v-model="pipeEditForm.mapping_silver_id" >
                                        <SelectTrigger class="w-full rounded border px-2 py-1">
                                            <SelectValue placeholder="— none —" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem v-for="x in mappingBS" :key="x.m.id" :value="x.m.id">
                                                #{{ x.m.id }} · {{ datasetsById[x.m.from_dataset_id]?.name }} → {{ datasetsById[x.m.to_dataset_id]?.name }}
                                            </SelectItem >
                                        </SelectContent>
                                    </Select>
                                </div>
                                <div class="space-y-1">
                                    <Label>Mapping (Silver → Gold)</Label>
                                    <Select v-model="pipeEditForm.mapping_gold_id" >
                                        <SelectTrigger class="rounded border px-2 py-1 text-sm">
                                            <SelectValue placeholder="— none —" />
                                        </SelectTrigger>
                                        <SelectContent>
                                            <SelectItem  v-for="x in mappingSG" :key="x.m.id" :value="x.m.id">
                                                #{{ x.m.id }} · {{ datasetsById[x.m.from_dataset_id]?.name }} → {{ datasetsById[x.m.to_dataset_id]?.name }}
                                            </SelectItem >
                                        </SelectContent>
                                    </Select>
                                </div>
                            </div>

                            <DialogFooter>
                                <Button @click="savePipeline(() => (pipeEditOpen=false))">Save</Button>
                            </DialogFooter>
                        </DialogContent>
                    </Dialog>
                </CardContent>
            </Card>



            <!-- 6) RUNS -->
            <Card class="lg:col-span-3">
                <CardHeader class="flex flex-row items-center justify-between">
                    <CardTitle>Runs</CardTitle>
                    <Dialog v-model:open="showNewRun">
                        <DialogTrigger as-child><Button size="sm">New Run</Button></DialogTrigger>
                        <DialogContent class="sm:max-w-lg">
                            <DialogHeader><DialogTitle>New Run</DialogTitle></DialogHeader>
                            <div class="grid gap-4">
                                <div class="space-y-2">
                                    <Label>Pipeline</Label>
                                    <Select v-model="runForm.pipeline_id">
                                        <SelectTrigger class="w-full"><SelectValue placeholder="Select pipeline" /></SelectTrigger>
                                        <SelectContent><SelectGroup>
                                            <SelectItem v-for="p in project.pipelines" :key="p.id" :value="p.id">{{ p.name }}</SelectItem>
                                        </SelectGroup></SelectContent>
                                    </Select>
                                </div>
                            </div>
                            <DialogFooter><Button @click="newRun(() => (showNewRun=false))">Run</Button></DialogFooter>
                        </DialogContent>
                    </Dialog>
                </CardHeader>
                <CardContent>
                    <div v-if="project.runs.length === 0" class="text-sm text-muted-foreground">Aucun run.</div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-sm">
                            <thead class="text-left">
                            <tr class="border-b">
                                <th class="py-2 pr-2">#</th>
                                <th class="py-2 pr-2">State</th>
                                <th class="py-2 pr-2">Pipeline</th>
                                <th class="py-2 pr-2">Rows (src/silver/reject)</th>
                                <th class="py-2 pr-2">Rows gold</th>
                                <th class="py-2 pr-2">Snapshots</th>
                                <th class="py-2 pr-2">Actions</th>
                            </tr>
                            </thead>
                            <tbody>
                            <tr v-for="r in project.runs" :key="r.id" class="border-b align-top">
                                <td class="py-2 pr-2">{{ r.id }}</td>
                                <td class="py-2 pr-2">
                                    <Badge :variant="r.state==='succeeded' ? 'secondary' : (r.state==='failed' ? 'destructive' : 'default')">{{ r.state }}</Badge>
                                    <div v-if="r.state_reason" class="text-xs text-muted-foreground max-w-xs truncate">{{ r.state_reason }}</div>
                                </td>
                                <td class="py-2 pr-2">{{ r.pipeline?.name ?? r.pipeline_id }}</td>
                                <td class="py-2 pr-2">
                                    <div>{{ r.rows_source ?? '-' }} / {{ r.rows_silver ?? '-' }} / {{ r.rows_source_rejected ?? '-' }}</div>
                                </td>
                                <td class="py-2 pr-2">{{ r.rows_gold ?? '-' }}</td>
                                <td class="py-2 pr-2 text-xs">
                                    <div>bronze: <span class="font-mono">{{ r.bronze_snapshot ?? '-' }}</span></div>
                                    <div>silver: <span class="font-mono">{{ r.silver_snapshot ?? '-' }}</span></div>
                                    <div>gold: <span class="font-mono">{{ r.gold_snapshot ?? '-' }}</span></div>
                                </td>
                                <td class="py-2 pr-2">
                                    <div class="flex flex-wrap gap-2">
                                        <Button size="sm" variant="outline" @click="openErrors(r)" :disabled="(r.errors?.length ?? 0)===0">Errors</Button>
                                    </div>
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </CardContent>
            </Card>
        </div>

        <!-- SCHEMA MODAL (amélioré) -->
        <Dialog v-model:open="schemaModal.open">
            <DialogContent class="w-[90vw] max-w-[90vw] h-[75vh]">
                <DialogHeader>
                    <DialogTitle >Source schema — #{{ schemaModal.sourceId }}</DialogTitle>
                </DialogHeader>

                <div class="h-full flex flex-col overflow-hidden">
                    <div v-if="schemaModal.error" class="text-sm text-red-600">
                        {{ schemaModal.error }}
                    </div>

                    <div v-else-if="!schemaModal.schema" class="text-sm text-muted-foreground">
                        No schema available.
                    </div>

                    <div v-else class="flex-1 min-h-0 grid grid-cols-2 gap-4 overflow-hidden">
                        <!-- Bronze -->
                        <section class="rounded border flex flex-col min-h-0">
                            <div class="p-2 text-sm font-medium border-b text-gray-800 bg-gray-50">Bronze (source)</div>
                            <div class="flex-1 overflow-auto p-2 text-xs">
                                <table class="w-full border-collapse">
                                    <thead class="sticky top-0 bg-white text-gray-800 z-10">
                                    <tr>
                                        <th class="text-left p-1 border-b">Field</th>
                                        <th class="text-left p-1 border-b">Type</th>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    <tr v-for="f in schemaModal.schema" :key="f.name" class="border-b">
                                        <td class="py-1 px-1">{{ f.name }}</td>
                                        <td class="py-1 px-1 text-muted-foreground">{{ f.type }}</td>
                                    </tr>
                                    </tbody>
                                </table>
                            </div>
                        </section>

                        <!-- Silver -->
                        <section v-if="silver?.schema?.length" class="rounded border flex flex-col min-h-0">
                            <div class="p-2 text-sm font-medium border-b text-gray-800  bg-gray-50">Silver (target)</div>
                            <div class="flex-1 overflow-auto p-2 text-xs">
                                <table class="w-full border-collapse">
                                    <thead class="sticky top-0 bg-white z-10 text-gray-800">
                                    <tr>
                                        <th class="text-left p-1 border-b">Field</th>
                                        <th class="text-left p-1 border-b">Type</th>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    <tr v-for="f in silver!.schema!" :key="f.name" class="border-b">
                                        <td class="py-1 px-1">
                                            {{ f.name }}
                                            <span
                                                v-if="!schemaModal.schema.find(s => s.name === f.name)"
                                                class="ml-1 text-[10px] text-orange-600"
                                            >
                                            no source
                                        </span>
                                        </td>
                                        <td class="py-1 px-1 text-muted-foreground">{{ f.type }}</td>
                                    </tr>
                                    </tbody>
                                </table>
                            </div>
                        </section>
                    </div>

                    <div class="pt-3 text-right border-t mt-2">
                        <Button variant="secondary" @click="schemaModal.open=false">Close</Button>
                    </div>
                </div>
            </DialogContent>
        </Dialog>

        <!-- ERROR SAMPLES MODAL -->
        <Dialog v-model:open="errorModal.open">
            <DialogContent class="max-w-5xl">
                <DialogHeader><DialogTitle>Error samples — Run #{{ errorModal.run?.id }}</DialogTitle></DialogHeader>
                <div v-if="!errorModal.run || (errorModal.run.errors?.length ?? 0) === 0" class="text-sm text-muted-foreground">Aucun échantillon d’erreur.</div>
                <div v-else class="max-h-[60vh] overflow-auto rounded border">
                    <table class="w-full text-xs">
                        <thead><tr class="border-b"><th class="text-left py-2 px-2">Row</th><th class="text-left py-2 px-2">Reason</th><th class="text-left py-2 px-2">Message</th><th class="text-left py-2 px-2">Values</th></tr></thead>
                        <tbody>
                        <tr v-for="e in errorModal.run!.errors" :key="e.id" class="border-b align-top">
                            <td class="py-2 px-2">{{ e.row_no ?? '-' }}</td>
                            <td class="py-2 px-2"><Badge variant="destructive">{{ e.reason_code }}</Badge></td>
                            <td class="py-2 px-2">{{ e.message }}</td>
                            <td class="py-2 px-2"><div class="font-mono whitespace-pre-wrap break-all">{{ JSON.stringify(e.source_values, null, 2) }}</div></td>
                        </tr>
                        </tbody>
                    </table>
                </div>
                <DialogFooter><Button variant="secondary" @click="errorModal.open=false">Close</Button></DialogFooter>
            </DialogContent>
        </Dialog>
    </AppLayout>
</template>
