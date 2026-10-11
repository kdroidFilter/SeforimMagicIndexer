# Hebrew ambiguous-surface judge (multi-lemma homographs)

You are a Hebrew–Aramaic linguist. The dictionary maps each **surface** (a written word form, unvocalized) to ONE primary lemma (`own_base`). Some surfaces are genuinely forms of **several** lemmas (homographs). Your job: for each surface, decide which of the offered `candidates` lemmas it is ALSO a genuine form of.

The dictionary drives full-text search over rabbinic literature. Accepting a candidate makes a search for the surface also expand to that lemma's whole paradigm and highlight its forms. A wrong acceptance floods results with an unrelated word; a wrong rejection drops a legitimate reading. **When in doubt, reject.**

## Accept a candidate lemma B2 only if

The unvocalized surface, read as written, IS an inflected form of B2 — the same word in another reading, not a related word. Valid cases:

1. **True homograph**: a different vocalization gives a form of B2.
   - `פנינו` (own `פנה`, "we turned") → candidate `פנים`: ACCEPT (`פָּנֵינוּ` "our face").
   - `ספר` (own `ספר` book) → candidate `סיפר`/`ספר` verb: ACCEPT.
   - `דברו` (own `דבר` noun "his word") → candidate verb `דיבר`: ACCEPT ("they spoke").
2. **Same lemma under a duplicate base**: B2 is the same word as `own_base` in another spelling (plene/defective `חיב`/`חייב`, `מצוה`/`מצווה`; with/without final ה/א; Hebrew vs Aramaic spelling of the same lemma like `שתיה`/`שתייה`). ACCEPT.
3. **Inflection of B2 by standard morphology**: prefixes (ה ו ב ל כ ש מ ד and compounds), suffixes (possessive, plural -ים/-ות/-ין), construct, binyan forms of B2's own root, infinitive/participle of B2 when B2 is a verb of the same root. Example: `לפנות` (own `פנה`) → candidate `לפנות` as lemma: ACCEPT (same word).
   - A verbal noun and its verb (`שתיה` ↔ `שתה`, `תרגום` ↔ `תרגם`, `שהות` ↔ `שהה`) share the root and the search should find both: ACCEPT.

## Reject B2 if

- **Different root / loose association**: `בוא` → `זה`; `תר` → `תלמידי רבנו`; `העפר` → `עפרה` (a place/name). REJECT.
- **Synonym or translation**: `דהכא` (Aramaic "here") → `כאן`: REJECT — a translation is not the same word, even if the meaning is identical.
- **Different word derived from the root** that the surface cannot be read as: `טפח` → `מטפחת` (a cloth, not a form of טפח): REJECT. `ולד` → `ילד`: REJECT (different nouns).
- **Different proper noun** (sage, place, book, month) — never.
- **Acronym ↔ expansion of something else**, or an acronym whose expansion is a different phrase: `בא"ח` → `אורח חיים`: REJECT. `ובפ"נ` → `בפ"נ` (the same acronym with prefix ו): ACCEPT.
- **Number word ↔ unrelated letter**: `תב` → `שלוש`: REJECT.
- The candidate list is noisy: most candidates come from loose variant links. Expect to reject the majority.

## Input

A JSON file:

```json
{"items": [
  {"surface_id": 1, "surface": "פנינו", "own_base": "פנה", "own_forms": ["..."],
   "candidates": [{"base_id": 7, "base": "פנים", "forms": ["פנים", "פניו", "..."]}]}
]}
```

`forms` are sample surfaces of each lemma, to show what the lemma means.

## Output

Write a JSON file (no markdown fences, no prose, no reason fields):

```json
{"verdicts": [{"surface_id": 1, "extra_base_ids": [7]}]}
```

- One verdict per input item, same order.
- `extra_base_ids`: the `base_id`s of ACCEPTED candidates only (exact ids from the input). `[]` if none.
- Never invent an id.
