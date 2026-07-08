# PLAN_TRANSFER_ORG.md — Transfer to `bouine-cache` GitHub Organization

One-time plan to transfer `thylong/bouine` and `thylong/bouine-documentation`
to the `https://github.com/bouine-cache` organization, update the Go module
path, Docker Hub image, Artifact Hub listing, Helm chart metadata, CI
workflows, documentation site, and all downstream references — without
breaking existing users.

The newly acquired `bouine.org` domain replaces all `thylong.com`
subdomains for user-facing surfaces (docs site, Helm chart repo).

Scope spans two repos:

| Repo | Current | Target |
|------|---------|--------|
| `thylong/bouine` | `github.com/thylong/bouine` | `github.com/bouine-cache/bouine` |
| `thylong/bouine-documentation` | `github.com/thylong/bouine-documentation` | `github.com/bouine-cache/bouine-documentation` |

External surfaces — current vs target:

| Surface | Current | Target |
|---------|---------|--------|
| Docker Hub image | `docker.io/thylong/bouine` | `docker.io/bouinecache/bouine` (§D1) |
| Helm chart repo | `https://charts.thylong.com` (gh-pages) | `https://charts.bouine.org` (§D2) |
| Artifact Hub listing | repo `bouine`, ID `ef3bdb5e-…` | re-register with new chart URL (§D3) |
| Docs site | `https://bouine.thylong.com` (k3s) | `https://bouine.org` (§D4) |
| GitHub redirects | `thylong/*` → `bouine-cache/*` | automatic, but time-limited |

Execute phases in order. Each has explicit exit criteria.

---

## Decision Matrix (resolved)

### D1 — Docker Hub image name → `bouinecache/bouine`

GitHub transfers `thylong/bouine` → `bouine-cache/bouine` automatically and
sets up redirects. Docker Hub does **not** support cross-account transfers.

**Decision: create `bouinecache` Docker Hub account.** ✅ Done.
- New image: `docker.io/bouinecache/bouine`
- Docker Hub org `bouinecache` and repo `bouine` created.
- Old `thylong/bouine` stays as a redirect (manual: retag + push, or leave
  stale with a deprecation notice).
- Cleanest for long-term, org-owned ownership.

**Impact:** `release.yml` DOCKER_IMAGE, Chart.yaml
artifacthub.io/images, values.yaml image.repository, README badge, all
docs `docker pull` / `docker run` commands.

### D2 — Helm chart repository URL → `charts.bouine.org`

`charts.thylong.com` is a custom domain CNAME'd to the `gh-pages` branch of
`thylong/bouine`. After transfer, `gh-pages` lives at
`bouine-cache.github.io/bouine` unless a custom domain is configured.

**Decision: use `charts.bouine.org` as the GitHub Pages custom domain.**
- Add a CNAME file containing `charts.bouine.org` to the `gh-pages` branch.
- Create a DNS CNAME record: `charts.bouine.org` → `bouine-cache.github.io`.
- GitHub Pages will serve the Helm repo at `https://charts.bouine.org`.
- Update all `helm repo add` instructions to the new URL.
- Artifact Hub must be re-registered with the new URL (see D3).

### D3 — Artifact Hub repository → update URL within `bouine` org

Artifact Hub tracks the chart repo by URL. The `bouine` organization and
`bouine` repository have been created on artifacthub.io (transferred from
the personal account to the org). Since the chart URL changes from
`charts.thylong.com` to `charts.bouine.org`, the repository URL must be
updated on artifacthub.io within the `bouine` org:

1. Update the existing repository URL to `https://charts.bouine.org`
   (Settings → Repository URL on artifacthub.io).
2. Update `artifacthub-repo.yml` owners to reference the org.
3. Re-verify the "Verified Publisher" badge with the new URL (Artifact Hub
   re-checks the `artifacthub-repo.yml` file at the new chart repo URL).
4. Configure the `bouine` org profile on artifacthub.io: display name,
   logo, description, links to GitHub and docs site.
5. Optionally remove the old `charts.thylong.com` listing once the new one
   is verified, or leave it with a deprecation notice.

The `repositoryID` (`ef3bdb5e-36fd-470e-856a-46288c8c248b`) stays the
same — it is tied to the repository, not the org or URL.

### D4 — Documentation site domain → `bouine.org`

`bouine.thylong.com` is hosted on the innerspace k3s cluster, independent
of GitHub. The transfer is an opportunity to move to the branded domain.

**Decision: move docs to `https://bouine.org`.**
- DNS: create an A/CNAME record for `bouine.org` pointing at the same
  origin as `bouine.thylong.com` (the innerspace k3s cluster IP, currently
  `51.159.109.21`).
- Cloudflare: add `bouine.org` to the Cloudflare account (proxied,
  orange-cloud). The wildcard `*.bouine.org` will also cover
  `charts.bouine.org` if DNS is set up at the apex.
- TLS: the existing `*.thylong.com` wildcard cert does not cover
  `bouine.org`. Obtain a new cert for `bouine.org` (Let's Encrypt via
  Traefik/Ingress, or a Cloudflare Origin CA cert).
- Ingress: add a new `Host(bouine.org)` route to the existing
  `bouine-docs` IngressRoute, alongside the old `bouine.thylong.com` host
  (keep both during the transition, remove the old one after verification).
- Hugo `baseURL`: change to `https://bouine.org/`.
- Keep `bouine.thylong.com` as a secondary host + HTTP 301 redirect to
  `bouine.org` during the transition period (at least 30 days).

---

## Phase 0 — Pre-flight (no changes)

- [ ] **Create the `bouine-cache` organization** on GitHub
      (`https://github.com/organizations/new`).
- [x] **Create `bouinecache` Docker Hub account and `bouine` repo.** ✅ Done.
      Generate a new access token. Store as
      `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` (will be set as
      GitHub Actions secrets after the transfer).
- [x] **Create `bouine` organization on artifacthub.io and transfer the
      `bouine` repository to it.** ✅ Done.
- [ ] **Configure `bouine.org` DNS:**
      - Add `bouine.org` to Cloudflare (proxied).
      - Create A record: `bouine.org` → `51.159.109.21` (innerspace origin).
      - Create CNAME record: `charts.bouine.org` → `bouine-cache.github.io`.
      - Create CNAME record: `www.bouine.org` → `bouine.org` (or apex A
        record, depending on Cloudflare setup).
- [ ] **Full backup of both repos** (mirror clones):
      ```bash
      git clone --mirror git@github.com:thylong/bouine.git               ~/backups/bouine-pre-transfer.git
      git clone --mirror git@github.com:thylong/bouine-documentation.git ~/backups/bouine-doc-pre-transfer.git
      ```
- [ ] **Snapshot the current `gh-pages` branch** of `thylong/bouine`
      (the Helm chart repo). This is the Artifact Hub source.
- [ ] **Record current state:**
      - Go module path: `github.com/thylong/bouine`
      - Docker image: `docker.io/thylong/bouine`
      - Chart repo: `https://charts.thylong.com`
      - Artifact Hub repo ID: `ef3bdb5e-36fd-470e-856a-46288c8c248b`
      - Docs site: `https://bouine.thylong.com`
      - Reference count: ~394 `github.com/thylong/bouine` refs in bouine,
        ~35 in bouine-documentation (content + config + generated public/).

**Exit:** org created, Docker Hub account created, `bouine.org` DNS
configured, backups done.

---

## Phase 1 — Transfer repos on GitHub

GitHub transfers preserve stars, issues, PRs, releases, branches, and
set up automatic redirects from the old URL.

### 1.1 Transfer `bouine`

- [ ] GitHub → Settings → Danger Zone → Transfer ownership.
      From: `thylong/bouine` → To: `bouine-cache/bouine`.
- [ ] Verify redirect: `git ls-remote git@github.com:thylong/bouine.git`
      still resolves (HTTP 301 to the new location).
- [ ] Verify all branches survived: `main`, `gh-pages`, `rewrite`.
- [ ] Verify tags survived: `v0.1.5` through `v0.1.9`.

### 1.2 Transfer `bouine-documentation`

- [ ] Transfer `thylong/bouine-documentation` → `bouine-cache/bouine-documentation`.
- [ ] Verify redirect works.

### 1.3 Update local remotes

```bash
# bouine
git remote set-url origin git@github.com:bouine-cache/bouine.git
# bouine-documentation
cd ../bouine-documentation
git remote set-url origin git@github.com:bouine-cache/bouine-documentation.git
```

### 1.4 Re-enable org-level features

- [ ] GitHub Actions: verify workflows are enabled on the transferred
      repos (GitHub disables Actions on transfer by default for security).
      Settings → Actions → General → Allow all actions.
- [ ] GitHub Pages: configure the `gh-pages` branch to serve at the custom
      domain `charts.bouine.org`. Settings → Pages → Custom domain →
      enter `charts.bouine.org`. Ensure the CNAME file is committed to
      `gh-pages`.
- [ ] Branch protection rules: re-create on `main` (protection rules do
      not survive org transfers).
- [ ] GitHub Secrets: re-add `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN` to
      the new `bouine-cache/bouine` repo (secrets do not survive
      transfers). Use the new `bouinecache` Docker Hub account creds.
- [ ] Environments: re-create any deployment environments if used.
- [ ] Discussions: re-enable if previously enabled.
- [ ] Repo metadata: set Homepage URL to `https://bouine.org`.

**Exit:** both repos accessible at `github.com/bouine-cache/*`, redirects
from `thylong/*` work, Actions enabled, secrets re-added, Pages serving
at `charts.bouine.org`.

---

## Phase 2 — Update Go module path (bouine)

The module path changes from `github.com/thylong/bouine` to
`github.com/bouine-cache/bouine`. This is a mechanical find-and-replace
across **114 Go files** plus config files.

### 2.1 go.mod

```diff
-module github.com/thylong/bouine
+module github.com/bouine-cache/bouine
```

### 2.2 All Go imports (114 files)

```bash
# Run from repo root
find . -name "*.go" -not -path "./vendor/*" -exec \
  sed -i '' 's|github.com/thylong/bouine|github.com/bouine-cache/bouine|g' {} +
```

### 2.3 Dockerfile (ldflags)

```diff
-      -X github.com/thylong/bouine/internal/buildinfo.Version=${VERSION} \
-      -X github.com/thylong/bouine/internal/buildinfo.Commit=${COMMIT} \
-      -X github.com/thylong/bouine/internal/buildinfo.Date=${DATE}" \
+      -X github.com/bouine-cache/bouine/internal/buildinfo.Version=${VERSION} \
+      -X github.com/bouine-cache/bouine/internal/buildinfo.Commit=${COMMIT} \
+      -X github.com/bouine-cache/bouine/internal/buildinfo.Date=${DATE}" \
```

### 2.4 Makefile (ldflags)

```diff
-                 -X github.com/thylong/bouine/internal/buildinfo.Version=$(VERSION) \
-                 -X github.com/thylong/bouine/internal/buildinfo.Commit=$(COMMIT) \
-                 -X github.com/thylong/bouine/internal/buildinfo.Date=$(DATE)
+                 -X github.com/bouine-cache/bouine/internal/buildinfo.Version=$(VERSION) \
+                 -X github.com/bouine-cache/bouine/internal/buildinfo.Commit=$(COMMIT) \
+                 -X github.com/bouine-cache/bouine/internal/buildinfo.Date=$(DATE)
```

### 2.5 `.golangci.yaml`

Three categories of references:
1. `goimports.local-prefixes`: `github.com/thylong/bouine` → `github.com/bouine-cache/bouine`
2. `depguard` allowed-deps lists: ~50 lines of `github.com/thylong/bouine/...`
3. Any other module-path references.

```bash
sed -i '' 's|github.com/thylong/bouine|github.com/bouine-cache/bouine|g' .golangci.yaml
```

### 2.6 `lint/depguard.yaml`

Same mechanical replacement:
```bash
sed -i '' 's|github.com/thylong/bouine|github.com/bouine-cache/bouine|g' lint/depguard.yaml
```

### 2.7 Verify the build

```bash
go mod tidy
go build ./...
go test -race -short ./...
make lint
```

**Exit:** `go build ./...` succeeds, `go test -race -short ./...` passes,
`make lint` clean, `go.mod` module path is `github.com/bouine-cache/bouine`.

---

## Phase 3 — Update CI workflows (bouine)

### 3.1 `.github/workflows/release.yml`

- [ ] `DOCKER_IMAGE` env var → `bouinecache/bouine`.
- [ ] ldflags `-X github.com/thylong/bouine/...` → `-X github.com/bouine-cache/bouine/...`
      (two occurrences in the multi-arch build step).

```diff
-env:
-  DOCKER_IMAGE: thylong/bouine
+env:
+  DOCKER_IMAGE: bouinecache/bouine
```

### 3.2 `.github/workflows/chart-release.yml`

- [ ] No module-path references, but verify the `gh-pages` branch serves
      at `charts.bouine.org` after the org transfer (Phase 1.4).
- [ ] Update comments referencing `charts.thylong.com` → `charts.bouine.org`.

### 3.3 `.github/workflows/ci.yml`

- [ ] No `thylong` references. No changes needed.
- [ ] Verify it runs on the transferred repo (Actions must be enabled,
      see Phase 1.4).

### 3.4 `.github/workflows/auto-rebase.yml`

```diff
-    if: github.repository == 'thylong/bouine'
+    if: github.repository == 'bouine-cache/bouine'
```

**Exit:** all workflow files reference the new org, CI passes on a
push to `main`.

---

## Phase 4 — Update Docker Hub (bouine)

- [x] Create Docker Hub account `bouinecache`. ✅ Done.
- [x] Create a new repo `bouinecache/bouine` (public). ✅ Done.
- [ ] Generate an access token; add to GitHub Actions secrets on
      `bouine-cache/bouine` as `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN`.
- [ ] Optional: on the old `thylong/bouine` Docker Hub repo, add a
      deprecation notice in the README pointing to `bouinecache/bouine`.
- [ ] Update all references (see Phase 5 and Phase 6 below).

**Exit:** `docker pull bouinecache/bouine:latest` works, release workflow
can push (verify with a test tag or a dry-run).

---

## Phase 5 — Update Helm chart & Artifact Hub (bouine)

### 5.1 `deploy/helm/bouine/Chart.yaml`

```diff
-icon: https://raw.githubusercontent.com/thylong/bouine/main/web/dashboard/logo.png
+icon: https://raw.githubusercontent.com/bouine-cache/bouine/main/web/dashboard/logo.png
...
-home: https://github.com/thylong/bouine
-sources:
-  - https://github.com/thylong/bouine
+home: https://github.com/bouine-cache/bouine
+sources:
+  - https://github.com/bouine-cache/bouine
...
-maintainers:
-  - name: thylong
-    url: https://github.com/thylong
+maintainers:
+  - name: bouine-cache
+    url: https://github.com/bouine-cache
```

Artifact Hub annotations:
```diff
     artifacthub.io/links: |
       - name: source
-        url: https://github.com/thylong/bouine
+        url: https://github.com/bouine-cache/bouine
       - name: documentation
-        url: https://bouine.thylong.com
+        url: https://bouine.org
       - name: container image
-        url: https://hub.docker.com/r/thylong/bouine
+        url: https://hub.docker.com/r/bouinecache/bouine
     artifacthub.io/images: |
       - name: bouine
-        image: docker.io/thylong/bouine:latest
+        image: docker.io/bouinecache/bouine:latest
```

Bump `version` and `appVersion` (chart-releaser skips versions it has
already published):
```diff
-version: 0.1.2
-appVersion: "0.1.2"
+version: 0.1.3
+appVersion: "0.1.3"
```

### 5.2 `deploy/helm/bouine/values.yaml`

```diff
 image:
-  repository: thylong/bouine
+  repository: bouinecache/bouine
   tag: ""
```

### 5.3 `deploy/helm/artifacthub-repo.yml`

```diff
 owners:
-  - name: thylong
-    email: theotime@protonmail.com
+  - name: bouine-cache
+    email: <new-contact-email>
```

The `repositoryID` (`ef3bdb5e-36fd-470e-856a-46288c8c248b`) stays the same —
it is tied to the repository, not the org. The repository has already been
transferred to the `bouine` org on artifacthub.io.

The chart URL must be updated on artifacthub.io from
`https://charts.thylong.com` to `https://charts.bouine.org` (Settings →
Repository URL). After updating, Artifact Hub will re-verify the
`artifacthub-repo.yml` at the new URL to confirm ownership.

### 5.4 Configure `charts.bouine.org` as GitHub Pages custom domain

- [ ] Add a CNAME file to the `gh-pages` branch containing:
      ```
      charts.bouine.org
      ```
- [ ] In GitHub repo settings → Pages → Custom domain, enter
      `charts.bouine.org` and enforce HTTPS.
- [ ] Verify DNS: `dig charts.bouine.org` resolves to
      `bouine-cache.github.io`.

### 5.5 Update Artifact Hub org profile and repository URL

- [x] Create `bouine` organization on artifacthub.io. ✅ Done.
- [x] Transfer `bouine` repository to the `bouine` org. ✅ Done.
- [ ] Configure the `bouine` org profile on artifacthub.io:
      display name, logo, description, links (GitHub, docs site).
- [ ] Update the repository URL on artifacthub.io from
      `https://charts.thylong.com` to `https://charts.bouine.org`.
- [ ] Verify the "Verified Publisher" badge re-activates after the URL
      change (Artifact Hub re-checks `artifacthub-repo.yml` at the new URL).
- [ ] Update the README Artifact Hub badge URL if the package path
      changed (the badge endpoint
      `https://artifacthub.io/badge/repository/bouine` may need updating to
      include the org name — verify on artifacthub.io).

### 5.6 Re-publish the chart

- [ ] Tag a new release (e.g. `v0.1.3`) to trigger `chart-release.yml`.
- [ ] Verify the new chart appears at `https://charts.bouine.org/index.yaml`.
- [ ] On artifacthub.io: verify the new version appears under the `bouine`
      org repository with updated links (source → `bouine-cache/bouine`,
      docs → `bouine.org`, container image → `bouinecache/bouine`).
- [ ] Optionally remove the old `charts.thylong.com` listing on
      artifacthub.io, or leave it with a deprecation notice.

**Exit:** `helm repo add bouine https://charts.bouine.org && helm install
bouine bouine/bouine` works and pulls the correct image. Artifact Hub
shows the new version under the `bouine` org with correct metadata.

---

## Phase 6 — Update bouine repo docs & metadata

### 6.1 `README.md`

```diff
-  <a href="https://github.com/thylong/bouine/actions/workflows/ci.yml"><img src="https://github.com/thylong/bouine/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
-  <a href="https://github.com/thylong/bouine/actions/workflows/release.yml"><img src="https://github.com/thylong/bouine/actions/workflows/release.yml/badge.svg" alt="Release"></a>
-  <a href="https://github.com/thylong/bouine/releases/latest"><img src="https://img.shields.io/github/v/release/thylong/bouine" alt="Latest Release"></a>
-  <a href="https://hub.docker.com/r/thylong/bouine"><img src="https://img.shields.io/docker/v/thylong/bouine?logoColor=blue&color=blue" alt="Docker"></a>
+  <a href="https://github.com/bouine-cache/bouine/actions/workflows/ci.yml"><img src="https://github.com/bouine-cache/bouine/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
+  <a href="https://github.com/bouine-cache/bouine/actions/workflows/release.yml"><img src="https://github.com/bouine-cache/bouine/actions/workflows/release.yml/badge.svg" alt="Release"></a>
+  <a href="https://github.com/bouine-cache/bouine/releases/latest"><img src="https://img.shields.io/github/v/release/bouine-cache/bouine" alt="Latest Release"></a>
+  <a href="https://hub.docker.com/r/bouinecache/bouine"><img src="https://img.shields.io/docker/v/bouinecache/bouine?logoColor=blue&color=blue" alt="Docker"></a>
```

Clone instructions:
```diff
-git clone https://github.com/thylong/bouine.git
+git clone https://github.com/bouine-cache/bouine.git
```

Docs link:
```diff
-📖 **Full documentation: [bouine.thylong.com](https://bouine.thylong.com)**
+📖 **Full documentation: [bouine.org](https://bouine.org)**
```

### 6.2 `CONTRIBUTING.md`

```diff
-git clone https://github.com/thylong/bouine.git
+git clone https://github.com/bouine-cache/bouine.git
```

### 6.3 `SECURITY.md`

```diff
-[Private vulnerability reporting](https://github.com/thylong/bouine/security/advisories/new)
+[Private vulnerability reporting](https://github.com/bouine-cache/bouine/security/advisories/new)
```

### 6.4 `deploy/grafana/bouine-red.json`

This is an example dashboard, not a runtime config. The
`namespace="thylong-innerspace"` label is environment-specific. Leave as-is
or update to a generic namespace if desired (not blocking).

### 6.5 `docs/plans/open-sourcing.md`

This is a historical document. Add a note at the top pointing to this
plan for the org transfer. Do not rewrite it.

**Exit:** no `thylong/bouine` references remain in bouine's user-facing
docs (except historical plan documents).

---

## Phase 7 — Update bouine-documentation repo

### 7.1 `hugo.toml`

```diff
-baseURL = 'https://bouine.thylong.com/'
+baseURL = 'https://bouine.org/'
...
-    docsRepo = 'https://github.com/thylong/bouine-documentation'
+    docsRepo = 'https://github.com/bouine-cache/bouine-documentation'
...
-      sameAs = ['https://github.com/thylong/bouine', 'https://hub.docker.com/r/thylong/bouine']
+      sameAs = ['https://github.com/bouine-cache/bouine', 'https://hub.docker.com/r/bouinecache/bouine']
...
-    url = 'https://github.com/thylong/bouine'
+    url = 'https://github.com/bouine-cache/bouine'
```

### 7.2 `README.md`

```diff
-Source for the [bouine](https://github.com/thylong/bouine) documentation site
-at **https://bouine.thylong.com**.
+Source for the [bouine](https://github.com/bouine-cache/bouine) documentation site
+at **https://bouine.org**.
...
-git clone git@github.com:thylong/bouine-documentation.git
+git clone git@github.com:bouine-cache/bouine-documentation.git
```

### 7.3 `Makefile`

The `NAMESPACE := thylong-innerspace` is the k3s cluster namespace.
This is environment-specific and does not change with the GitHub org
transfer or domain move. Leave as-is unless the cluster namespace is
also being renamed.

### 7.4 `.github/workflows/build.yml`

```diff
-        run: hugo --environment production --minify --gc --baseURL "https://bouine.thylong.com/"
+        run: hugo --environment production --minify --gc --baseURL "https://bouine.org/"
```

### 7.5 Content files (`content/docs/**/*.md`)

All GitHub URLs, Docker image references, and domain references need
updating:

| File | References to update |
|------|---------------------|
| `content/docs/configuration/helm.md` | GitHub source link, `helm repo add` URL → `charts.bouine.org`, `image.repository` default |
| `content/docs/contributing/_index.md` | `git clone` URL |
| `content/docs/contributing/security.md` | Security advisories URL |
| `content/docs/operations/troubleshooting.md` | Docker build command, runbook GitHub links (4) |
| `content/docs/operations/observability.md` | Grafana namespace label (env-specific — leave) |
| `content/docs/getting-started/kubernetes.md` | Chart repo URL → `charts.bouine.org`, image reference |
| `content/docs/getting-started/install.md` | Docker pull/run, GitHub Releases URL, git clone, docs link → `bouine.org` |
| `content/docs/getting-started/docker.md` | Docker image name (3 occurrences) |

```bash
# From bouine-documentation root
find content/ -name "*.md" -exec \
  sed -i '' 's|github.com/thylong/bouine|github.com/bouine-cache/bouine|g' {} +
# Docker image
find content/ -name "*.md" -exec \
  sed -i '' 's|thylong/bouine|bouinecache/bouine|g' {} +
# Chart repo URL
find content/ -name "*.md" -exec \
  sed -i '' 's|charts.thylong.com|charts.bouine.org|g' {} +
# Docs site URL
find content/ -name "*.md" -exec \
  sed -i '' 's|bouine.thylong.com|bouine.org|g' {} +
```

### 7.6 Regenerate `public/`

The `public/` directory contains pre-built static HTML. After updating
content, rebuild:

```bash
npm run build
# or
hugo --environment production --minify --gc --baseURL "https://bouine.org/"
```

Commit the regenerated `public/` directory.

### 7.7 Verify the docs build

```bash
cd ../bouine-documentation
npm run build
```

Check for broken links (the Hugo build will warn on dead internal links).
Manually verify a few external links (GitHub repo, Docker Hub, chart repo).

**Exit:** `npm run build` succeeds, no `thylong/bouine` GitHub URLs or
`thylong.com` domain references remain in `content/` or `hugo.toml`,
`public/` regenerated.

---

## Phase 8 — Move docs site to `bouine.org`

### 8.1 TLS certificate

The existing `*.thylong.com` wildcard cert does not cover `bouine.org`.
Obtain a new certificate:

- [ ] Option A: Let's Encrypt via the cluster's Traefik IngressRoute
      (automatic ACME HTTP-01 or TLS-ALPN challenge for `bouine.org`).
- [ ] Option B: Cloudflare Origin CA cert for `bouine.org` (15-year
      validity, accepted by Cloudflare in strict SSL mode).
- [ ] Verify the cert is valid: `openssl s_client -connect 51.159.109.21:443 -servername bouine.org`.

### 8.2 Ingress / IngressRoute

In the innerspace k8s manifests (private repo):

- [ ] Add `Host(bouine.org)` to the existing `bouine-docs` IngressRoute,
      alongside `Host(bouine.thylong.com)` (keep both during transition).
- [ ] Add a redirect rule: `Host(bouine.thylong.com)` → 301 →
      `Host(bouine.org)` (to be activated after verification, or
      immediately if preferred).
- [ ] Apply: `kubectl apply -k ../../innerspace/k8s/`.
- [ ] Verify: `curl -H "Host: bouine.org" https://bouine.org/` returns 200.

### 8.3 Cloudflare

- [ ] SSL mode: strict (origin must present a valid cert for `bouine.org`).
- [ ] Verify the Cloudflare proxy is working: check response headers for
      `cf-ray` and `server: cloudflare`.
- [ ] Add a redirect rule in Cloudflare: `bouine.thylong.com/*` →
      `bouine.org/$1` (Page Rule or Redirect Rule). This ensures old
      links redirect even after the k3s redirect is removed.

**Exit:** `https://bouine.org` serves the docs over a valid TLS cert
through Cloudflare. `https://bouine.thylong.com` redirects to
`https://bouine.org`.

---

## Phase 9 — Post-transfer verification

### 9.1 CI pipeline

- [ ] Push a trivial commit to `bouine-cache/bouine` `main`.
- [ ] Verify CI runs: `pre-commit`, `lint`, `govulncheck`, `test`, `build`,
      `bench`, `conformance` — all green.
- [ ] Verify the `auto-rebase.yml` guard fires (repo name check).

### 9.2 Release dry-run

- [ ] Tag a pre-release (e.g. `v0.1.3-rc.1`).
- [ ] Verify `release.yml` builds binaries for all platforms.
- [ ] Verify Docker image is pushed to `bouinecache/bouine` on Docker Hub.
- [ ] Verify `chart-release.yml` packages and publishes the chart.
- [ ] Verify the chart appears at `https://charts.bouine.org/index.yaml`.
- [ ] Verify Artifact Hub picks up the new version from the new URL.

### 9.3 User-facing checks

- [ ] `git clone https://github.com/thylong/bouine.git` still works
      (redirect). Verify it lands on `bouine-cache/bouine`.
- [ ] `docker pull bouinecache/bouine:latest` works.
- [ ] `helm repo add bouine https://charts.bouine.org && helm install bouine bouine/bouine`
      works and the pod starts.
- [ ] `https://bouine.org` serves the updated docs.
- [ ] `https://bouine.thylong.com` redirects to `https://bouine.org`.
- [ ] `https://charts.bouine.org/index.yaml` lists the new chart version.
- [ ] Artifact Hub page shows the new version and updated links
      (source, docs, container image).
- [ ] README badges render correctly (CI, Release, Docker, Artifact Hub).

### 9.4 Go module consumer check

- [ ] In a throwaway Go project:
      ```bash
      go mod init test
      go get github.com/bouine-cache/bouine@latest
      ```
      Verify it resolves. Old `go get github.com/thylong/bouine` should
      also work via GitHub redirect + Go's vanity path resolution, but
      the canonical path is now `bouine-cache/bouine`.

**Exit:** all checks green, existing users experience zero downtime.

---

## Phase 10 — Cleanup

- [ ] Remove old GitHub secrets from `thylong` account (if any remain).
- [ ] Mark old Docker Hub `thylong/bouine` as deprecated.
- [ ] After 30 days of stable redirect, remove `bouine.thylong.com` from
      the IngressRoute (keep the Cloudflare redirect rule indefinitely).
- [ ] After 30 days, remove the old `charts.thylong.com` listing on
      Artifact Hub (or leave with deprecation notice).
- [ ] Update any external links pointing to `thylong/bouine`:
      - Personal GitHub profile pinned repos
      - Social media / blog posts
      - Any monitoring dashboards that reference the repo URL
- [ ] Archive the old `docs/plans/open-sourcing.md` with a pointer to
      this plan.
- [ ] Keep this plan file as a historical record.

---

## Summary of files to change

### bouine (114 Go files + config)

| File | Change |
|------|--------|
| `go.mod` | module path |
| `**/*.go` (114 files) | import paths |
| `Dockerfile` | ldflags |
| `Makefile` | ldflags |
| `.golangci.yaml` | local-prefixes + depguard |
| `lint/depguard.yaml` | allowed-deps |
| `.github/workflows/release.yml` | DOCKER_IMAGE → `bouinecache/bouine` + ldflags |
| `.github/workflows/chart-release.yml` | comments referencing `charts.thylong.com` |
| `.github/workflows/auto-rebase.yml` | repo name guard |
| `deploy/helm/bouine/Chart.yaml` | icon, home, sources, maintainers, artifacthub annotations (incl. docs URL → `bouine.org`), version bump |
| `deploy/helm/bouine/values.yaml` | image.repository → `bouinecache/bouine` |
| `deploy/helm/artifacthub-repo.yml` | owners → `bouine-cache` org |
| `README.md` | badges, clone URL, docs link → `bouine.org` |
| `CONTRIBUTING.md` | clone URL |
| `SECURITY.md` | advisories URL |

### bouine-documentation (~35 refs)

| File | Change |
|------|--------|
| `hugo.toml` | baseURL → `bouine.org`, docsRepo, sameAs, menu URL |
| `README.md` | GitHub URLs, docs URL → `bouine.org` |
| `.github/workflows/build.yml` | baseURL → `bouine.org` |
| `content/docs/**/*.md` (8 files) | GitHub URLs, Docker image, chart repo URL → `charts.bouine.org`, docs URL → `bouine.org` |
| `public/` | regenerate via `npm run build` |

### Infrastructure (innerspace k8s, private)

| Item | Change |
|------|--------|
| DNS | `bouine.org` A record, `charts.bouine.org` CNAME, `www.bouine.org` |
| Cloudflare | add `bouine.org` zone, SSL strict, redirect `bouine.thylong.com` → `bouine.org` |
| TLS cert | new cert for `bouine.org` (LE or Cloudflare Origin CA) |
| IngressRoute | add `Host(bouine.org)`, add redirect from `bouine.thylong.com` |
| GitHub Pages | CNAME file `charts.bouine.org` on `gh-pages` branch |
| Artifact Hub | update repository URL to `charts.bouine.org`, configure `bouine` org profile |

---

*Last updated: 2026-07-08*
