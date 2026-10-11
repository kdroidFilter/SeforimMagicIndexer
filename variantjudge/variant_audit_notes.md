# Variant Removal Audit Log

Independent Claude QA of `MainJudgeVariants` decisions.
Driven by `.claude/commands/audit-variants.md`, triggered on `/loop 10m /audit-variants`.

Cursor (last audited `removed_at` ms): see `.audit_cursor` next to this file.

Legend:
- ✅ legit removal (MiniMax was right)
- ⚠️ false removal (MiniMax was wrong — variant should be re-inserted)
- 🚨 systematic pattern (≥3 similar false removals in a batch)

---

## 2026-06-17 17:20 — sample 20 of 7862 removals

- DB: `/home/elie-gambache/IdeaProjects/SeforimApp/SeforimLibrary/build/lexical.db`
- Window: cursor=1777040876389 → max_ts=1777177778340
- Verdict: 19 legit ✅ / 1 suspicious ⚠️
- Rolling precision (this batch): 19/20 = 95%

### ✅ Legit removals
- Different root / fabricated / semantic-only noise: `נשם` for `אישה`, `תורא` for `אורייתא`, `מם` for `ימין`, `רב` for `כרוב`, `הקלחות` for `מקלחת`, `בא` for `אבא`, `בחו` for `חוץ לארץ`.
- Numeric mismatches: `2` for `ו` (=6), `אחד` for `כט` (=29), `עשרה` for `י"ו` (=16), `מלשון` for `מל'` (=30), the malformed long `ר"ת...ש` for `רטו` (=215).
- Ambiguous acronym expansions correctly rejected when they point to a different phrase/entity than the assigned base: `ערב שבת` for `ע"ש` (=עיין שם), `שולחן דוד` for `ש"ד`, `פרק יין` for `פר"י`, `משה רב יוסף לנדא` for `מהרי"ל`.
- Synonym / phrase drift: `בעד` for `בעבור`, `כולם` for `כ`/`עשרים`, `המוטיר` for `מטיר`.

### ⚠️ Suspicious
- `surface="למכיר"` / `base="מכיר"` → removed `"הכיר"`
  - Verdict: FALSE — same root family; HR/rule 11 keeps infinitive/finite/participle variants of the same root (`מכיר` ↔ `הכיר`).

### Notes
- The prior systemic failures (base deletion, ש/מ prefix rejection, Aramaic 1:1 cognate rejection, plene/defective plural rejection) did not appear in this sample.
- Precision is high enough for continued read-only auditing, but same-root finite/participle removals should stay on the watch list.

## 2026-04-24 17:20 — sample 20 of 26 removals

- Window: cursor=0 → max_ts=1777040876389
- Verdict: 7 legit ✅ / 13 suspicious ⚠️
- Rolling precision (this batch): 7/20 = 35%  — **well below the smoke-sample estimate of 71%**

### ⚠️ Suspicious (clear false removals)
- `surface="רגלים"` → removed `"רגליים"` (MiniMax: "צורה לא תקינה של השורש")
  - Verdict: FALSE — `רגליים` is the modern plene dual of `רגל`. Exact same lemma.
- `surface="ברגליו"` → removed `"ברגלים"` (MiniMax: "צורת רבים לא תקינה למעצם נקבה")
  - Verdict: FALSE — prefix ב- + legitimate plural `רגלים`. Valid form.
- `surface="לאחוז"` → removed `"אחז"` (MiniMax: "צורת יסוד בלי ל התחלתי")
  - Verdict: FALSE — `אחז` IS the declared base! Judge removed the lemma itself.
- `surface="שחציו"` → removed `"חצי"` (MiniMax: "ה-base עצמו ללא כל שיכול להתאים ל-surface")
  - Verdict: FALSE — same bug: removed the base.
- `surface="כסף"` → removed `"כספא"` (MiniMax: "צורה זרה לחלוטין")
  - Verdict: FALSE — `כספא` is the standard Aramaic cognate of `כסף` (Talmud). Rule 6 of Gemini prompt explicitly allows these 1:1 cognates.
- `surface="וחציו"` → removed `"שחציו"` (MiniMax: "תחילית לא מוכרת (ש-)")
  - Verdict: FALSE — ש- is a standard morphological prefix. Judge contradicted its own system prompt which lists ש explicitly.

### ⚠️ Borderline (judge over-strict, probably wrong)
- `surface="שאינם"` → removed `"כאינם"`, `"לאינם"`, `"מאינם"`, `"באינם"` (4 removals) — MiniMax: "X not in morphological prefix list"
  - Judge treats ה/ו/ב/ל/כ as the only strippable prefixes for this particular family, ignoring ש/מ despite its system prompt.
- `surface="שאינם"` → removed `"שאינו"`, `"שאינה"` — feminine/singular conjugations. Arguably legitimate removals per strict Rule 8 (no paradigm expansion), but contextually tight.
- `surface="שוטה"` → removed `"שטיה"` (Aramaic feminine cognate; debatable).

### 🚨 Patterns (critical)
1. **Base form removed from variants** (`אחז`, `חצי` × 2 instances) — the judge does not treat `variants[i] == base` as always-legit, violating Gemini prompt Rule 2. This is a systemic bug.
2. **Aramaic 1:1 cognates removed** (`כספא` for כסף; likely also `שטיא`-family) — judge ignores Rule 6 allowance.
3. **ש / מ prefixes treated as forbidden** (5+ removals against `שאינם`, 1 against `וחציו`) — despite the judge system prompt explicitly listing ש and מ, the model flags them as "not in the morphological prefix list". Model appears to mirror Gemini's Rule 2 literal list rather than our judge rules.
4. **Plene/defective of regular plurals** (`רגלים` ↔ `רגליים`, `ברגלים`) — judge rejects legitimate matres-lectionis variations.

### 🛑 Recommendation
STOP the background run. The judge prompt needs reinforcement before continuing. At 35% precision we are removing more legitimate variants than hallucinations. Priority fixes:
- Hard rule: the base value is ALWAYS legitimate; never flag it.
- Reinforce ש / מ as first-class prefixes (with explicit examples and counter-examples vs stopwords).
- Reinforce Aramaic↔Hebrew 1:1 cognates (list common patterns: -א suffix for Aramaic).
- Reinforce matres-lectionis tolerance for plurals (`רגלים`/`רגליים`, `מדות`/`מידות`).

---

## 2026-10-11 — corpus words finished, fragments purged, issue #3 (multi-lemma surfaces)

- Enrich: 8,500 pending corpus words judged by 85 Sonnet agents → 0 removals, 10,769 additions.
- Fragments: extract_corpus_words.py split abbreviations on gershayim (לפמ"ש → לפמ + ש).
  `corpus/find_fragments.py` rescanned seforim.db; 1,451 words with standalone ratio < 0.1
  deleted by `corpus/delete_fragments.py` (with their links, orphan variants and bases).
  Words above the threshold (נעה, הצדיקי) are real and kept. The extractor itself is not fixed yet.
- Issue #3: new table `surface_extra_base` (also in Database.sq). Candidates = surfaces linked as
  variants to surfaces of another base (`claude-batch/pull_ambiguous.py`): 33,014 surfaces / 68,982
  pairs, judged by 331 Sonnet agents (`ambiguous_prompt.md`) → 26,670 extra lemmas applied
  (`apply_ambiguous.py`, 144 pairs to non-Hebrew bases dropped). פנינו → פנה + פנים.
  Zayit reads it via `MagicDictionaryIndex.LOOKUP_WITH_EXTRA_BASES_SQL` (old DBs fall back).
- Release v0.4.1 asset renamed `lexical0.4.1.1.db` → `lexical.db` (DownloadLexicalDb requires that name).
