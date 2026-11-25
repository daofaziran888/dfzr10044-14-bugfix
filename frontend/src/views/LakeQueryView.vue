<template>
  <section class="page lake-query-page" data-module="lake_query" data-testid="lake-query-page">
    <header class="page-head">
      <div>
        <h2>入湖查询</h2>
        <p class="page-desc">按湖表查询已确认和已写入待回执的湖行；原批重放与更正重发均按确定性湖行标识幂等。</p>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="loadRows">
      <label class="filter-item">
        <span>所属湖表</span>
        <input v-model="lakeTable" data-testid="lake-query-table" placeholder="例如 ods_trade_amount_fix" />
      </label>
      <label class="filter-item">
        <span>amount</span>
        <input v-model="amountFilter" data-testid="lake-query-amount" placeholder="可选：0 / 25" />
      </label>
      <button class="btn primary" type="submit" data-testid="lake-query-submit">查询</button>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="lake-query-error">{{ errorMessage }}</p>

    <table class="data-table" data-testid="lake-query-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, index) in rows" :key="row._lake_row_id || index" :data-testid="`lake-row-${row.amount}`">
          <td v-for="column in columns" :key="column">{{ formatValue(row[column]) }}</td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length" class="empty-state">暂无入湖记录</td>
        </tr>
      </tbody>
    </table>
    <footer class="page-foot">共 {{ rows.length }} 行；amount=0 与 amount=25 应各只有一行</footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import { request } from '@/api/client'

const route = useRoute()
const lakeTable = ref(String(route.query.lake_table || 'ods_trade_amount_fix'))
const amountFilter = ref(String(route.query.amount || ''))
const rows = ref<Record<string, any>[]>([])
const errorMessage = ref('')

const columns = computed(() => {
  const preferred = [
    'trade_id', 'amount', '_lake_row_id', '_source_batch_id',
    '_source_row_id', '_receipt_id', '_commit_status',
  ]
  const present = new Set<string>()
  rows.value.forEach((row) => Object.keys(row).forEach((key) => present.add(key)))
  return [
    ...preferred.filter((key) => present.has(key)),
    ...[...present].filter((key) => !preferred.includes(key)),
  ]
})

function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

async function loadRows() {
  errorMessage.value = ''
  try {
    const params = new URLSearchParams({ lake_table: lakeTable.value })
    if (amountFilter.value) params.set('amount', amountFilter.value)
    const response = await request(`/api/lake/query?${params.toString()}`)
    const payload = await response.json()
    if (!response.ok || payload.ok === false) throw new Error(payload.error || '入湖查询失败')
    rows.value = payload.internal_items || payload.items || []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '入湖查询失败'
  }
}

onMounted(loadRows)
</script>
