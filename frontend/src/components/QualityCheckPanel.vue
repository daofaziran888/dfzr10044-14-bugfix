<template>
  <div class="quality-entry" data-testid="quality-check-panel">
    <div class="page-actions">
      <button
        class="btn primary"
        type="button"
        data-testid="open-quality-check"
        aria-label="质量检查"
        @click="openPanel"
      >
        质量检查入口
      </button>
      <span class="hint">缺列、null、0 三行可直接提交验收场景</span>
    </div>

    <div v-if="showPanel" class="modal-backdrop" data-testid="quality-check-modal">
      <form class="modal quality-modal" @submit.prevent="submit">
        <h3>从质量检查入口提交原批次</h3>
        <div class="form-grid">
          <label class="create-field">
            <span>所属湖表</span>
            <input v-model="lakeTable" data-testid="quality-lake-table" />
          </label>
          <label class="create-field">
            <span>批次编号</span>
            <input v-model="batchId" data-testid="quality-batch-id" />
          </label>
          <label class="create-field">
            <span>amount 非空规则编号</span>
            <input v-model="ruleId" data-testid="quality-rule-id" />
          </label>
          <label class="checkbox-field">
            <input v-model="lateReceipt" type="checkbox" data-testid="quality-late-receipt" />
            <span>原批确认回执迟到</span>
          </label>
        </div>
        <label class="create-field">
          <span>原批次行（JSON 数组；第一行缺 amount 列，第二行显式 null，第三行 amount=0）</span>
          <textarea
            v-model="rowsText"
            class="json-input"
            rows="9"
            data-testid="quality-rows"
          ></textarea>
        </label>
        <div class="page-actions">
          <button class="btn ghost" type="button" data-testid="fill-scenario" @click="fillScenario">
            填入三行验收数据
          </button>
          <button class="btn ghost" type="button" data-testid="submit-scenario" @click="submitScenario">
            一键提交三行场景
          </button>
          <button class="btn" type="button" @click="showPanel = false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-quality-check" :disabled="submitting">
            {{ submitting ? '检查并交接中…' : '提交原批次' }}
          </button>
        </div>
        <p v-if="errorMessage" class="error-text" data-testid="quality-error">{{ errorMessage }}</p>

        <section v-if="result" class="result-panel" data-testid="quality-result">
          <h4>质量判定与交接结果</h4>
          <p>
            共 {{ result.total_rows }} 行；通过 {{ result.clean_rows }} 行；
            隔离 {{ result.violation_rows }} 行。通过明细 amount：
            <strong data-testid="clean-amounts">{{ cleanAmounts.join(', ') || '（空）' }}</strong>
          </p>
          <div class="result-columns">
            <div>
              <h5>通过明细 / 已写入 0</h5>
              <pre data-testid="clean-detail">{{ JSON.stringify(result.clean_data, null, 2) }}</pre>
            </div>
            <div>
              <h5>非空规则隔离明细</h5>
              <ul class="link-list">
                <li v-for="qid in result.quarantine_record_ids" :key="qid">
                  <RouterLink
                    :to="`/quarantine_zone/detail/${encodeURIComponent(qid)}?lake_table=${encodeURIComponent(lakeTable)}`"
                    :data-testid="`open-quarantine-${qid}`"
                  >
                    打开隔离详情：{{ qid }}
                  </RouterLink>
                </li>
              </ul>
            </div>
          </div>
          <div v-if="lakeRows.length" class="lake-query-mini" data-testid="quality-lake-rows">
            <h5>入湖查询</h5>
            <table class="data-table compact">
              <thead><tr><th v-for="key in lakeColumns" :key="key">{{ key }}</th></tr></thead>
              <tbody>
                <tr v-for="(row, index) in lakeRows" :key="index">
                  <td v-for="key in lakeColumns" :key="key">{{ row[key] ?? '—' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
      </form>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, ref } from 'vue'

import { qualityCheck, queryLakeRows } from '@/api/backend-service'

const emit = defineEmits<{ submitted: [] }>()

const lakeTable = ref('ods_trade_amount_fix')
const batchId = ref('BATCH-AMOUNT-FIX-001')
const ruleId = ref('QR-AMOUNT-NOT-NULL')
const lateReceipt = ref(true)
const rowsText = ref('')
const showPanel = ref(false)
const submitting = ref(false)
const errorMessage = ref('')
const result = ref<any>(null)
const lakeRows = ref<Record<string, unknown>[]>([])

const scenarioRows = [
  { trade_id: 'T-MISSING-AMOUNT', currency: 'CNY' },
  { trade_id: 'T-NULL-AMOUNT', currency: 'CNY', amount: null },
  { trade_id: 'T-ZERO-AMOUNT', currency: 'CNY', amount: 0 },
]

const cleanAmounts = computed(() =>
  Array.isArray(result.value?.clean_data)
    ? result.value.clean_data.map((row: Record<string, unknown>) => String(row.amount))
    : [],
)

const lakeColumns = computed(() => {
  const keys = new Set<string>()
  lakeRows.value.forEach((row) => Object.keys(row).forEach((key) => keys.add(key)))
  return [...keys]
})

function fillScenario() {
  rowsText.value = JSON.stringify(scenarioRows, null, 2)
}

function openPanel() {
  if (!rowsText.value) fillScenario()
  errorMessage.value = ''
  showPanel.value = true
}

async function submitScenario() {
  fillScenario()
  showPanel.value = true
  await nextTick()
  await submit()
}

async function submit() {
  submitting.value = true
  errorMessage.value = ''
  try {
    let rows: unknown
    try {
      rows = JSON.parse(rowsText.value)
    } catch {
      throw new Error('行数据不是合法 JSON 数组')
    }
    const response = await qualityCheck({
      lake_table: lakeTable.value,
      batch_id: batchId.value,
      late_receipt: lateReceipt.value,
      rule: {
        规则编号: ruleId.value,
        规则名称: 'amount 非空规则',
        校验列: 'amount',
        校验类型: 'NOT_NULL',
        违规处置: '隔离',
      },
      rows,
    })
    if (!response.ok) throw new Error(response.error || response.message || '质量检查未通过')
    result.value = response
    const lake = await queryLakeRows(lakeTable.value)
    lakeRows.value = lake.items || []
    emit('submitted')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '质量检查失败'
  } finally {
    submitting.value = false
  }
}

fillScenario()
</script>

<style scoped>
.quality-entry { margin-bottom: 12px; }
.hint { color: var(--text-muted, #6b7280); font-size: 13px; }
.quality-modal { max-width: 980px; width: min(96vw, 980px); max-height: 92vh; overflow: auto; }
.form-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
.checkbox-field { display: flex; align-items: center; gap: 8px; padding-top: 24px; }
.json-input { width: 100%; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.result-panel { margin-top: 16px; border-top: 1px solid #e5e7eb; padding-top: 12px; }
.result-columns { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.result-columns pre { max-height: 220px; overflow: auto; background: #f8fafc; padding: 8px; border-radius: 6px; }
.link-list { margin: 0; padding-left: 18px; }
.lake-query-mini { margin-top: 12px; }
.compact { font-size: 13px; }
</style>
