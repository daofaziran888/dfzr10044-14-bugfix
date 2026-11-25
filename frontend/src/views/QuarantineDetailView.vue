<template>
  <section class="page quarantine-detail" data-module="quarantine_zone_detail" data-testid="quarantine-detail-page">
    <header class="page-head">
      <div>
        <h2>隔离详情 · 重试回炉</h2>
        <p class="page-desc">
          原始隔离记录保留可查；更正通过复检后取得独立更正入湖资格，回执指向实际确认的湖行。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/quarantine_zone">返回隔离区列表</RouterLink>
        <RouterLink class="btn" to="/lake_query">打开入湖查询</RouterLink>
      </div>
    </header>

    <p v-if="loading" data-testid="detail-loading">隔离详情加载中…</p>
    <p v-else-if="errorMessage" class="error-text" data-testid="detail-error">{{ errorMessage }}</p>

    <template v-else-if="detail">
      <div class="detail-grid">
        <article class="detail-card" data-testid="quarantine-association">
          <h3>关联信息</h3>
          <dl>
            <div><dt>隔离编号</dt><dd data-testid="detail-quarantine-id">{{ quarantineCode }}</dd></div>
            <div><dt>记录标识</dt><dd data-testid="detail-record-id">{{ detail.quarantine_record_id }}</dd></div>
            <div><dt>所属湖表</dt><dd data-testid="detail-lake-table">{{ detail.lake_table }}</dd></div>
            <div><dt>来源批次</dt><dd data-testid="detail-batch-id">{{ detail.batch_id }}</dd></div>
            <div><dt>原始行标识</dt><dd data-testid="detail-source-row">{{ detail.source_row_id }}</dd></div>
            <div><dt>关联标识</dt><dd data-testid="detail-correlation-id">{{ retryResult?.correlation_id || '复检后生成' }}</dd></div>
            <div><dt>违规规则</dt><dd data-testid="detail-rule">{{ detail.rule_id }} {{ detail.rule_name }}</dd></div>
            <div><dt>当前状态</dt><dd data-testid="detail-status">{{ detail.status || detail.entry?.status }}</dd></div>
            <div v-if="detail.batch_receipt">
              <dt>原批回执</dt>
              <dd data-testid="original-batch-receipt">
                {{ detail.batch_receipt.receipt_status }} / {{ detail.batch_receipt.receipt_id || '无回执编号' }}
              </dd>
            </div>
          </dl>
          <button class="btn" type="button" data-testid="register-late-receipt" @click="registerLateReceipt">
            登记原批迟到回执并对账
          </button>
          <p v-if="receiptMessage" class="hint" data-testid="late-receipt-message">{{ receiptMessage }}</p>
        </article>

        <article class="detail-card">
          <h3>原始隔离记录</h3>
          <div v-if="amountMissing" class="warning" data-testid="missing-amount-warning">
            原始行缺少 amount 列；质量规则已按非空违规隔离，而不是放行。
          </div>
          <div v-else-if="amountIsNull" class="warning" data-testid="null-amount-warning">
            原始行 amount 为显式 null；未补录前继续隔离。
          </div>
          <pre data-testid="original-row">{{ JSON.stringify(detail.original_row, null, 2) }}</pre>
        </article>
      </div>

      <article class="detail-card correction-card">
        <h3>更正后复检 / 回炉</h3>
        <p class="hint" data-testid="lineage-hint">
          回炉更正通过「所属湖表 + 来源批次 + 原始行标识」与同一批次的原始行关联；错用其他湖表的同编号批次会被拒绝。
        </p>
        <div class="correction-form">
          <label class="create-field">
            <span>更正 amount</span>
            <input v-model="amountText" data-testid="correction-amount" inputmode="decimal" />
          </label>
          <label class="create-field grow">
            <span>幂等/更正编号（可空；回执丢失后重发同一更正请保持不变）</span>
            <input v-model="idempotencyKey" data-testid="correction-idempotency-key" />
          </label>
          <button
            class="btn"
            type="button"
            data-testid="start-recheck"
            :disabled="submitting"
            @click="submitCorrection(false)"
          >
            发起复检
          </button>
          <button
            class="btn primary"
            type="button"
            data-testid="retry-reflow"
            :disabled="submitting"
            @click="submitCorrection(false)"
          >
            {{ submitting ? '复检并提交中…' : '重试回炉' }}
          </button>
          <button
            class="btn"
            type="button"
            data-testid="resend-correction"
            :disabled="submitting || !lastPayload"
            @click="submitCorrection(true)"
          >
            回执丢失，重发原更正
          </button>
        </div>
        <p v-if="actionError" class="error-text" data-testid="retry-error">{{ actionError }}</p>

        <section v-if="retryResult?.receipt" class="receipt-panel" data-testid="correction-receipt">
          <h4>更正回执 <span v-if="retryResult.duplicate" class="dup">（幂等重放，未新增行）</span></h4>
          <dl>
            <div><dt>回执编号</dt><dd data-testid="receipt-id">{{ retryResult.receipt.receipt_id }}</dd></div>
            <div><dt>回炉作业</dt><dd data-testid="reflow-job-id">{{ retryResult.reflow_job?.reflow_job_id }}</dd></div>
            <div><dt>入湖资格</dt><dd data-testid="ingest-qualification">{{ retryResult.reflow_job?.qualification }}</dd></div>
            <div><dt>湖行标识</dt><dd data-testid="confirmed-lake-row-id">{{ retryResult.lake_row_id }}</dd></div>
            <div><dt>关联批次/原始行</dt><dd>{{ retryResult.batch_id }} / {{ retryResult.source_row_id }}</dd></div>
          </dl>
          <h5>实际确认的记录</h5>
          <pre data-testid="confirmed-record">{{ JSON.stringify(retryResult.receipt.confirmed_record, null, 2) }}</pre>
        </section>
      </article>

      <article class="detail-card">
        <h3>入湖查询</h3>
        <button class="btn" type="button" data-testid="refresh-lake" @click="loadLake">刷新入湖查询</button>
        <table class="data-table" data-testid="detail-lake-table">
          <thead>
            <tr><th>trade_id</th><th>amount</th><th>来源批次</th><th>原始行</th><th>回执</th></tr>
          </thead>
          <tbody>
            <tr v-for="(row, index) in lakeInternalRows" :key="index">
              <td>{{ row.trade_id ?? '—' }}</td>
              <td data-testid="lake-amount">{{ row.amount }}</td>
              <td>{{ row._source_batch_id }}</td>
              <td>{{ row._source_row_id }}</td>
              <td>{{ row._receipt_id || '原批迟到/待回执' }}</td>
            </tr>
            <tr v-if="!lakeInternalRows.length"><td colspan="5" class="empty-state">暂无入湖记录</td></tr>
          </tbody>
        </table>
      </article>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import { getQuarantineDetail, queryLakeRows, receiveBatchReceipt, retryQuarantine } from '@/api/backend-service'

const route = useRoute()
const detail = ref<any>(null)
const loading = ref(true)
const errorMessage = ref('')
const actionError = ref('')
const submitting = ref(false)
const retryResult = ref<any>(null)
const amountText = ref('25')
const idempotencyKey = ref('')
const lastPayload = ref<Record<string, unknown> | null>(null)
const lakeRows = ref<any[]>([])
const receiptMessage = ref('')

const recordId = computed(() => {
  if (route.params.id) return String(route.params.id)
  const queryId = route.query.id || route.query.quarantine_id || route.query.quarantine_record_id
  return Array.isArray(queryId) ? String(queryId[0] || '') : String(queryId || '')
})
const queryLakeTable = computed<string | undefined>(() => {
  const value = route.query.lake_table
  if (Array.isArray(value)) return value[0] ?? undefined
  return value === null ? undefined : value
})
const quarantineCode = computed(() => detail.value?.entry?.隔离编号 || detail.value?.隔离编号 || recordId.value)
const amountMissing = computed(() => detail.value?.original_row && !('amount' in detail.value.original_row))
const amountIsNull = computed(() => detail.value?.original_row?.amount === null)
const lakeInternalRows = computed(() => {
  // queryLakeRows returns business rows without internal fields; use internal endpoint data through
  // a second shape if available. The API helper currently returns items, so enrich by fetch result.
  return lakeRows.value
})

async function loadDetail() {
  if (!recordId.value) {
    loading.value = false
    errorMessage.value = '缺少隔离记录编号'
    return
  }
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await getQuarantineDetail(recordId.value, queryLakeTable.value)
    if (!response.ok) {
      errorMessage.value = response.error || '隔离详情不存在'
    } else {
      detail.value = response
      if (!idempotencyKey.value) {
        idempotencyKey.value = `CORR-UI-${recordId.value.replace(/[^A-Za-z0-9]/g, '').slice(-16).toUpperCase()}`
      }
      await loadLake()
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '隔离详情读取失败'
  } finally {
    loading.value = false
  }
}

async function loadLake() {
  const table = detail.value?.lake_table || queryLakeTable.value
  if (!table) return
  try {
    const response = await queryLakeRows(table)
    // The public query strips internal fields; request internal_items when present.
    lakeRows.value = response.internal_items || response.items || []
  } catch {
    lakeRows.value = []
  }
}

function normalizedAmount(): unknown {
  const text = amountText.value.trim()
  if (text === '') return null
  const integer = Number.parseInt(text, 10)
  if (/^-?\d+$/.test(text) && Number.isSafeInteger(integer)) return integer
  const number = Number(text)
  return Number.isFinite(number) ? number : text
}

async function registerLateReceipt() {
  if (!detail.value) return
  receiptMessage.value = ''
  actionError.value = ''
  try {
    const response = await receiveBatchReceipt({
      lake_table: detail.value.lake_table || queryLakeTable.value,
      batch_id: detail.value.batch_id,
    })
    if (!response.ok) throw new Error(response.error || '原批回执登记失败')
    receiptMessage.value = '原批迟到回执已登记并完成对账；回炉更正仍使用独立幂等回执。'
    await loadDetail()
  } catch (error) {
    actionError.value = error instanceof Error ? error.message : '原批回执登记失败'
  }
}

async function submitCorrection(replaySame: boolean) {
  if (!detail.value) return
  actionError.value = ''
  submitting.value = true
  try {
    const payload = replaySame && lastPayload.value
      ? lastPayload.value
      : {
          lake_table: detail.value.lake_table || queryLakeTable.value,
          batch_id: detail.value.batch_id,
          source_row_id: detail.value.source_row_id,
          quarantine_record_id: detail.value.quarantine_record_id,
          idempotency_key: idempotencyKey.value || undefined,
          values: { amount: normalizedAmount() },
        }
    lastPayload.value = payload
    const response = await retryQuarantine(
      detail.value.entry?.id || recordId.value,
      payload,
    )
    if (!response.ok) {
      actionError.value = response.error || response.message || '回炉复检失败，记录继续隔离'
      return
    }
    retryResult.value = response
    await loadDetail()
  } catch (error) {
    actionError.value = error instanceof Error ? error.message : '回炉请求失败'
  } finally {
    submitting.value = false
  }
}

watch(() => [route.params.id, route.query.id, route.query.quarantine_record_id], loadDetail)
onMounted(loadDetail)
</script>

<style scoped>
.detail-grid { display: grid; grid-template-columns: minmax(280px, 0.8fr) minmax(360px, 1.2fr); gap: 16px; }
.detail-card { background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
.detail-card dl { display: grid; gap: 8px; margin: 0; }
.detail-card dl > div { display: grid; grid-template-columns: 110px 1fr; gap: 8px; }
.detail-card dt { color: #6b7280; }
.detail-card dd { margin: 0; word-break: break-all; }
.detail-card pre { background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 8px; overflow: auto; }
.warning { background: #fef3c7; border: 1px solid #f59e0b; color: #92400e; padding: 10px; border-radius: 8px; margin-bottom: 10px; }
.hint { color: #64748b; font-size: 13px; margin: 0 0 12px; }
.correction-form { display: flex; gap: 12px; align-items: end; flex-wrap: wrap; }
.correction-form .grow { flex: 1; min-width: 260px; }
.receipt-panel { margin-top: 16px; border-top: 1px solid #e5e7eb; padding-top: 12px; }
.receipt-panel pre { background: #ecfdf5; color: #064e3b; }
.dup { color: #b45309; font-size: 13px; }
@media (max-width: 900px) { .detail-grid { grid-template-columns: 1fr; } }
</style>
