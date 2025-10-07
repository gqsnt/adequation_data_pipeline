<!-- components/pipeline/SilverSchemaEditor.vue (version généralisée) -->
<script setup lang="ts">
import { Button } from '../ui/button';
import { Input } from '../ui/input';
import {
    Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '../ui/select';
import { Switch } from '../ui/switch';
import { ref, watch, computed } from 'vue';

type Field = { name: string; type: string; nullable: boolean };

const props = defineProps<{
    title?: string;
    initial?: Field[];
    initialPrimaryKey?: string[];
    onSave: (payload: { fields: Field[]; primary_key: string[] }) => Promise<void>;
}>();

const rows = ref<Field[]>(props.initial?.map(f => ({ ...f })) ?? []);
const pk = ref<string[]>(props.initialPrimaryKey ?? []);

watch(() => props.initial, (v) => {
    rows.value = v?.map(f => ({ ...f })) ?? [];
}, { deep: true });

watch(() => props.initialPrimaryKey, (v) => { pk.value = v ?? []; });

const types = ['str', 'f64', 'i64', 'bool', 'date', 'datetime'];

const addField = () => rows.value.push({ name: '', type: 'str', nullable: true });
const removeField = (i: number) => rows.value.splice(i, 1);

function togglePk(name: string, v: boolean) {
    const s = new Set(pk.value);
    if (v) s.add(name); else s.delete(name);
    pk.value = [...s];
}

const hasRows = computed(() => rows.value.length > 0);

const save = async () => {
    await props.onSave({ fields: rows.value, primary_key: pk.value });
};
</script>

<template>
    <div class="space-y-3">
        <div class="flex items-center justify-between">
            <h3 class="font-semibold">{{ title ?? 'Schéma' }}</h3>
            <div class="flex gap-2">
                <Button variant="secondary" @click="addField">Ajouter un champ</Button>
                <Button :disabled="!hasRows" @click="save">Enregistrer</Button>
            </div>
        </div>

        <div class="grid grid-cols-12 items-center gap-2 text-sm font-medium text-muted-foreground">
            <div class="col-span-4">Nom</div>
            <div class="col-span-3">Type</div>
            <div class="col-span-2">Nullable</div>
            <div class="col-span-2">PK</div>
            <div class="col-span-1"></div>
        </div>

        <div class="space-y-2 max-h-[60vh] overflow-auto pr-1">
            <div v-for="(r, i) in rows" :key="i" class="grid grid-cols-12 items-center gap-2">
                <div class="col-span-4">
                    <Input v-model="r.name" placeholder="ex: valeur_fonciere" />
                </div>
                <div class="col-span-3">
                    <Select v-model="r.type">
                        <SelectTrigger><SelectValue placeholder="Type"/></SelectTrigger>
                        <SelectContent>
                            <SelectItem v-for="t in types" :key="t" :value="t">{{ t }}</SelectItem>
                        </SelectContent>
                    </Select>
                </div>
                <div class="col-span-2">
                    <Switch v-model:checked="r.nullable" />
                </div>
                <div class="col-span-2">
                    <input type="checkbox" :checked="pk.includes(r.name)" @change="e => togglePk(r.name, (e.target as HTMLInputElement).checked)" />
                </div>
                <div class="col-span-1 text-right">
                    <Button size="sm" variant="ghost" @click="removeField(i)">Suppr.</Button>
                </div>
            </div>
        </div>
    </div>
</template>
