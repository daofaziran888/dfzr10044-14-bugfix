import { createRouter, createWebHistory } from 'vue-router'

import LakeQueryView from '@/views/LakeQueryView.vue'
import Overview from '@/views/Overview.vue'
import QuarantineDetailView from '@/views/QuarantineDetailView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/lake_query', name: 'lake-query', component: LakeQueryView },
    {
      path: '/quarantine_zone/detail',
      name: 'quarantine-detail-query',
      component: QuarantineDetailView,
      props: true,
    },
    {
      path: '/quarantine_zone/detail/:id',
      name: 'quarantine-detail-named',
      component: QuarantineDetailView,
      props: true,
    },
    {
      path: '/quarantine_zone/:id/detail',
      name: 'quarantine-detail',
      component: QuarantineDetailView,
      props: true,
    },
    {
      path: '/quarantine_zone/:id',
      name: 'quarantine-detail-short',
      component: QuarantineDetailView,
      props: true,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
