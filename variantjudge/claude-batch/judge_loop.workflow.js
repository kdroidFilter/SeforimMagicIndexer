export const meta = {
  name: 'variant-judge-sonnet-loop',
  description: 'Judge+enrich remaining unjudged Hebrew lexical variants in lexical.db using Sonnet subagents, 600/round (6x100), until exhausted',
  phases: [
    { title: 'Prep' },
    { title: 'Judge', model: 'sonnet' },
    { title: 'Apply' },
  ],
}

const VJ = '/home/elie-gambache/IdeaProjects/SeforimApp/SeforimLibrary/SeforimMagicIndexer/variantjudge'
const DB = '/home/elie-gambache/IdeaProjects/SeforimApp/SeforimLibrary/build/lexical.db'
const PROMPT = `${VJ}/claude-batch/subagent_prompt.md`
const MAX_ROUNDS = 150
// Pause (seconds) inserted after each round's apply step to keep the sustained
// request rate under the Sonnet rate limit (429/529 were exhausting retries).
const ROUND_PAUSE_SEC = 45

const PREP_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    runDir: { type: 'string' },
    chunks: { type: 'array', items: { type: 'string' } },
    itemCount: { type: 'integer' },
    remainingBefore: { type: 'integer' },
  },
  required: ['runDir', 'chunks', 'itemCount', 'remainingBefore'],
}

const APPLY_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    removed: { type: 'integer' },
    added: { type: 'integer' },
    judged: { type: 'integer' },
    remaining: { type: 'integer' },
  },
  required: ['removed', 'added', 'judged', 'remaining'],
}

let grandRemoved = 0
let grandAdded = 0
let grandJudged = 0
let prepFailStreak = 0
const MAX_PREP_FAILS = 6

for (let round = 1; round <= MAX_ROUNDS; round++) {
  phase('Prep')
  const prep = await agent(
    `You prepare a batch round for the Hebrew variant-judge pipeline. Run these bash steps and return ONLY the structured result.

1. Create a unique run directory:
   STAMP=$(date +%Y%m%d-%H%M%S)
   RUN="${VJ}/claude-batch/runs/\${STAMP}-r${round}-6x100"
   mkdir -p "$RUN/chunks" "$RUN/verdicts"
2. Pull up to 600 unjudged surfaces:
   python3 ${VJ}/claude-batch/pull_batch.py "${DB}" 600 > "$RUN/batch.json"
3. Count items:
   N=$(python3 -c "import json;print(len(json.load(open('$RUN/batch.json'))['items']))")
   If N is 0, return {"runDir": "$RUN", "chunks": [], "itemCount": 0} and STOP (do not split).
4. Otherwise split into chunks of 100:
   python3 ${VJ}/claude-batch/split_batch.py "$RUN/batch.json" "$RUN/chunks/chunk-" 100
   This prints the chunk file paths (chunk-001.json ...).
5. Compute the AUTHORITATIVE count of unjudged surfaces that still have variants (this is remainingBefore — do NOT confuse it with N):
   sqlite3 "${DB}" "SELECT COUNT(DISTINCT s.id) FROM surface s JOIN surface_variant sv ON sv.surface_id=s.id LEFT JOIN variant_judgment vj ON vj.surface_id=s.id WHERE vj.surface_id IS NULL;"
6. Return runDir, the chunk file paths (sorted), itemCount=N, and remainingBefore=<the sqlite count>.`,
    { label: `prep:r${round}`, phase: 'Prep', model: 'haiku', schema: PREP_SCHEMA },
  )

  // Robust termination: only stop when the DB authoritatively reports 0 unjudged
  // surfaces. A null prep (rate-limited/transient) or a spurious itemCount==0 with
  // work still remaining must NOT end the run — that bug stopped a prior run early.
  if (!prep) {
    prepFailStreak++
    log(`Round ${round}: prep agent returned no result (streak ${prepFailStreak}/${MAX_PREP_FAILS}). Retrying next round.`)
    if (prepFailStreak >= MAX_PREP_FAILS) {
      log(`Giving up after ${MAX_PREP_FAILS} consecutive prep failures.`)
      break
    }
    continue
  }
  prepFailStreak = 0
  if (prep.remainingBefore === 0) {
    log(`Round ${round}: DB reports 0 unjudged surfaces — done. Totals: ${grandJudged} judged, ${grandRemoved} removed, ${grandAdded} added.`)
    break
  }
  if (prep.itemCount === 0 || prep.chunks.length === 0) {
    log(`Round ${round}: pull returned 0 items but DB still has ${prep.remainingBefore} unjudged — transient; retrying next round.`)
    continue
  }

  log(`Round ${round}: pulled ${prep.itemCount} surfaces into ${prep.chunks.length} chunks. Judging with Sonnet...`)

  phase('Judge')
  await parallel(prep.chunks.map((chunkPath, i) => () => {
    const verdictPath = chunkPath
      .replace('/chunks/', '/verdicts/')
      .replace(/\.json$/, '.verdict.json')
    return agent(
      `You are a Hebrew-Aramaic lexical-variant judge AND enricher.

STEP 1: Read the FULL system prompt that defines your task and rules:
  Read tool on: ${PROMPT}
Follow it EXACTLY — the Hard Rules (HR1-HR6), the legitimate-variant classes (1-12), the F1-F4 flag codes, the A1-A9 enrichment rules, and the anti-hallucination rules.

STEP 2: Read your input chunk:
  Read tool on: ${chunkPath}

STEP 3: Produce the verdict JSON following the Output schema in the system prompt (one verdict per input item, same order; fields surface_id, surface, base, false_variants, additions; no reason fields; no markdown).

STEP 4: Write it with the Write tool to EXACTLY this path:
  ${verdictPath}

Return the one-line summary: wrote N verdicts to <path> (X removals, Y additions).`,
      { label: `judge:r${round}c${i + 1}`, phase: 'Judge', model: 'sonnet' },
    )
  }))

  phase('Apply')
  const result = await agent(
    `You apply the verdicts for run directory ${prep.runDir}. Run these bash steps and return ONLY the structured result.

1. Verify all verdict files exist (one per chunk). Expected chunk count: ${prep.chunks.length}.
   ls ${prep.runDir}/verdicts/*.verdict.json
   If any are missing, still proceed with the ones present but note it.
2. Merge all chunk verdicts into one file:
   python3 -c "
   import json, glob, os
   files=sorted(glob.glob('${prep.runDir}/verdicts/*.verdict.json'))
   merged=[]
   for f in files:
       merged += json.load(open(f, encoding='utf-8'))['verdicts']
   json.dump({'verdicts': merged}, open('${prep.runDir}/merged.verdict.json','w',encoding='utf-8'), ensure_ascii=False, indent=2)
   print('merged', len(merged), 'verdicts')
   "
3. Apply to the DB (this prints 'applied: J judged, R removed, A added ...' on stderr):
   python3 ${VJ}/claude-batch/apply_verdicts.py "${DB}" "${prep.runDir}/merged.verdict.json"
4. Query the remaining unjudged-with-variants count:
   sqlite3 "${DB}" "SELECT COUNT(DISTINCT s.id) FROM surface s JOIN surface_variant sv ON sv.surface_id=s.id LEFT JOIN variant_judgment vj ON vj.surface_id=s.id WHERE vj.surface_id IS NULL;"
5. Pace the loop to stay under the rate limit: sleep ${ROUND_PAUSE_SEC}
6. Parse the apply output and return: removed (R), added (A), judged (J), remaining (the count from step 4).`,
    { label: `apply:r${round}`, phase: 'Apply', model: 'haiku', schema: APPLY_SCHEMA },
  )

  if (result) {
    grandRemoved += result.removed
    grandAdded += result.added
    grandJudged += result.judged
    log(`Round ${round} applied: ${result.judged} judged (+${result.removed} removed, +${result.added} added). Remaining unjudged: ${result.remaining}. Cumulative: ${grandJudged} judged.`)
    if (result.remaining === 0) {
      log(`All surfaces judged. Done after ${round} rounds. Totals: ${grandJudged} judged, ${grandRemoved} removed, ${grandAdded} added.`)
      break
    }
  } else {
    log(`Round ${round}: apply step returned no result — investigate run ${prep.runDir}.`)
  }
}

return { grandJudged, grandRemoved, grandAdded }
