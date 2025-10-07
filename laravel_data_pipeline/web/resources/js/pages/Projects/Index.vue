<script setup lang="ts">
import AppLayout from '@/layouts/AppLayout.vue';
import { Head, router, useForm } from '@inertiajs/vue3';

// shadcn-vue UI
import { Button } from '../../components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '../../components/ui/dialog';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';

type Project = {
    id: string;
    slug: string;
    warehouse_uri: string;
    namespace: string;
    created_at: string;
};

defineProps<{ projects: Project[] }>();

const form = useForm({
    slug: '',
    warehouse_uri: 'file:///warehouse',
    namespace: 'dvf',
});

function createProject(close: () => void) {
    router.post('/projects', form, {
        onSuccess: () => { form.reset('slug'); close(); },
    });
}
</script>

<template>
    <Head title="Projects" />
    <AppLayout>
        <div class="mx-auto max-w-6xl space-y-6 p-6">
            <div class="flex items-center justify-between">
                <h1 class="text-2xl font-semibold">Projects</h1>

                <Dialog>
                    <DialogTrigger as-child><Button size="sm">New Project</Button></DialogTrigger>
                    <DialogContent class="sm:max-w-lg">
                        <DialogHeader><DialogTitle>Create Project</DialogTitle></DialogHeader>

                        <div class="space-y-4">
                            <div class="space-y-2">
                                <Label for="slug">Slug</Label>
                                <Input id="slug" v-model="form.slug" placeholder="ex: dvf-local" />
                            </div>
                            <div class="space-y-2">
                                <Label for="wh">Warehouse URI</Label>
                                <Input id="wh" v-model="form.warehouse_uri" placeholder="file:///warehouse" />
                            </div>
                            <div class="space-y-2">
                                <Label for="ns">Namespace</Label>
                                <Input id="ns" v-model="form.namespace" placeholder="dvf" />
                            </div>
                        </div>

                        <DialogFooter>
                            <Button variant="secondary" @click="form.reset()">Reset</Button>
                            <Button @click="createProject(() => null)">Create</Button>
                        </DialogFooter>
                    </DialogContent>
                </Dialog>
            </div>

            <div class="grid grid-cols-1 gap-4 md:grid-cols-2">
                <Card v-for="p in projects" :key="p.id" class="transition hover:shadow-md">
                    <CardHeader>
                        <CardTitle class="flex items-center justify-between">
                            <span>{{ p.slug }}</span>
                            <a :href="`/projects/${p.id}`" class="text-sm underline">Open</a>
                        </CardTitle>
                    </CardHeader>
                    <CardContent class="text-sm">
                        <div><span class="font-medium">Namespace:</span> {{ p.namespace }}</div>
                        <div class="truncate"><span class="font-medium">Warehouse:</span> {{ p.warehouse_uri }}</div>
                        <div class="text-muted-foreground">Created: {{ new Date(p.created_at).toLocaleString() }}</div>
                    </CardContent>
                </Card>
            </div>

            <div v-if="projects.length === 0" class="text-sm text-muted-foreground">
                Aucun projet. Créez-en un pour commencer.
            </div>
        </div>
    </AppLayout>
</template>
