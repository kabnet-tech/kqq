---
name: Feature request
about: Suggest a new capability for kqq
title: ''
labels: enhancement
assignees: ''
---

**The workload**
What data are you processing, and what are you trying to do with it? Describe
the job, not just the syntax.

**Input shape** (example):

```json
{"example": "record"}
```

**What you'd like to be able to write** (proposed syntax, if you have one):

```
kqq 'select ...'
```

**How you solve it today** (jq, Miller, a script, etc.):

**Is this blocking you from using kqq, or a nice-to-have?**

**Additional context**
kqq deliberately stays small — the bar for new syntax is "common enough that
data engineers hit it weekly on NDJSON streams." Explaining the workload helps
a lot.