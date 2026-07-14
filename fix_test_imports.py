import re
from pathlib import Path

test_root = Path('fe/fe-core/src/test/java')
repls = {
    'import com.google.api.client.util.Lists;': 'import com.google.common.collect.Lists;',
    'import com.google.api.client.util.Sets;': 'import com.google.common.collect.Sets;',
    'import com.google.api.client.util.Preconditions;': 'import com.google.common.base.Preconditions;',
    'com.google.api.client.util.Lists.': 'Lists.',
    'com.google.api.client.util.Sets.': 'Sets.',
    'com.google.api.client.util.Preconditions.': 'Preconditions.',
}

changed = []
for p in test_root.rglob('*.java'):
    text = p.read_text()
    new = text
    for a, b in repls.items():
        new = new.replace(a, b)
    if new != text:
        p.write_text(new)
        changed.append(p)

print(f'replaced imports in {len(changed)} test files')

# Sort com.google.* import blocks in all changed files
sort_fixed = []
for p in changed:
    lines = p.read_text().splitlines(keepends=True)
    new_lines = []
    i = 0
    modified = False
    while i < len(lines):
        if re.match(r'^import com\.google\.', lines[i]):
            run = []
            j = i
            while j < len(lines) and re.match(r'^import com\.google\.', lines[j]):
                run.append(lines[j])
                j += 1
            sorted_run = sorted(run)
            if sorted_run != run:
                modified = True
            new_lines.extend(sorted_run)
            i = j
        else:
            new_lines.append(lines[i])
            i += 1
    if modified:
        p.write_text(''.join(new_lines))
        sort_fixed.append(str(p))

print(f'sorted imports in {len(sort_fixed)} test files')
for c in sorted(str(p) for p in changed):
    print(' ', c)
