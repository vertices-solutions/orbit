set shell := ["sh", "-cu"]

release_state := ".release-state.env"

# Compute per-crate coverage with cargo-tarpaulin and print a summary table.
coverage:
	@mkdir -p target/tarpaulin/orbit target/tarpaulin/orbitd
	@command -v gum >/dev/null || { \
		echo "gum is required (https://github.com/charmbracelet/gum)"; \
		exit 1; \
	}
	@gum spin --spinner dot \
		--title "Running cargo-tarpaulin for orbit..." \
		-- sh -c 'cargo tarpaulin -p orbit --out Json --output-dir target/tarpaulin/orbit >target/tarpaulin/orbit/tarpaulin.log 2>&1' \
		|| { \
			echo "orbit tarpaulin failed"; \
			cat target/tarpaulin/orbit/tarpaulin.log; \
			exit 1; \
		}
	@gum spin --spinner dot \
		--title "Running cargo-tarpaulin for orbitd..." \
		-- sh -c 'cargo tarpaulin -p orbitd --out Json --output-dir target/tarpaulin/orbitd >target/tarpaulin/orbitd/tarpaulin.log 2>&1' \
		|| { \
			echo "orbitd tarpaulin failed"; \
			cat target/tarpaulin/orbitd/tarpaulin.log; \
			exit 1; \
		}
	@python3 scripts/coverage_summary.py

release:
	@rm -f {{release_state}}
	@just _release-check-commands
	@just _release-check-git
	@just _release-tests
	@just _release-select-version
	@just _release-select-remote
	@just _release-bump
	@just _release-version-docs
	@just _release-stage
	@just _release-commit
	@just _release-tag
	@just _release-push
	@rm -f {{release_state}}

[private]
_release-check-commands:
	@echo "==> Command checks"
	@for cmd in git cargo cargo-release python3 gum npm; do \
		command -v "$cmd" >/dev/null 2>&1 || { \
			echo "error: missing required command: $cmd" >&2; \
			exit 1; \
		}; \
	done

[private]
_release-check-git:
	@echo "==> Git state checks"
	@if [ ! -d .git ]; then \
		echo "error: not a git repository: $(pwd)" >&2; \
		exit 1; \
	fi
	@branch="$(git rev-parse --abbrev-ref HEAD)"; \
	if [ "$branch" != "main" ]; then \
		echo "error: release must be run on main (current: $branch)" >&2; \
		exit 1; \
	fi
	@if [ -n "$(git status --porcelain)" ]; then \
		echo "error: working tree is dirty; commit or stash changes before releasing." >&2; \
		git status -sb; \
		exit 1; \
	fi

[private]
_release-tests:
	@echo "==> Testing"
	@cargo test
	@python3 tests/run_e2e.py --cluster winery
	@if [ -n "$(git status --porcelain)" ]; then \
		echo "error: tests modified the working tree; clean up before releasing." >&2; \
		git status -sb; \
		exit 1; \
	fi

[private]
_release-select-version:
	@echo "==> Version selection"
	@orbit_version="$(awk '/^\[package\]/{in_pkg=1;next}/^\[/{in_pkg=0}in_pkg&&$1=="version"{v=$3;gsub(/"/,"",v);print v;exit}' orbit/Cargo.toml)"; \
	orbitd_version="$(awk '/^\[package\]/{in_pkg=1;next}/^\[/{in_pkg=0}in_pkg&&$1=="version"{v=$3;gsub(/"/,"",v);print v;exit}' orbitd/Cargo.toml)"; \
	proto_version="$(awk '/^\[package\]/{in_pkg=1;next}/^\[/{in_pkg=0}in_pkg&&$1=="version"{v=$3;gsub(/"/,"",v);print v;exit}' proto/Cargo.toml)"; \
	if [ "$orbit_version" = "$orbitd_version" ] && [ "$orbit_version" = "$proto_version" ]; then \
		base_version="$orbit_version"; \
	else \
		choice="$(printf '%s\n' "orbit:$orbit_version" "orbitd:$orbitd_version" "proto:$proto_version" | gum choose --header "Versions differ. Select base version to bump.")"; \
		base_version="${choice#*:}"; \
	fi; \
	echo "$base_version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || { \
		echo "error: version '$base_version' is not in X.Y.Z format" >&2; \
		exit 1; \
	}; \
	major="${base_version%%.*}"; \
	rest="${base_version#*.}"; \
	minor="${rest%%.*}"; \
	patch="${rest#*.}"; \
	major_next="$((major + 1)).0.0"; \
	minor_next="$major.$((minor + 1)).0"; \
	patch_next="$major.$minor.$((patch + 1))"; \
	choice="$(gum choose --header "Current version: $base_version" "major:$major_next" "minor:$minor_next" "patch:$patch_next")"; \
	new_version="${choice#*:}"; \
	echo "NEW_VERSION=$new_version" > {{release_state}}; \
	echo "TAG=v$new_version" >> {{release_state}}

[private]
_release-select-remote:
	@. {{release_state}}; \
	remote="$(git config --get branch.main.remote || true)"; \
	if [ -z "$remote" ]; then \
		remotes="$(git remote)"; \
		count="$(printf '%s\n' "$remotes" | sed '/^$/d' | wc -l | tr -d ' ')"; \
		if [ "$count" -eq 0 ]; then \
			echo "error: no git remotes configured." >&2; \
			exit 1; \
		elif [ "$count" -eq 1 ]; then \
			remote="$remotes"; \
		else \
			remote="$(printf '%s\n' "$remotes" | gum choose --header "Select remote to push")"; \
		fi; \
	fi; \
	echo "NEW_VERSION=$NEW_VERSION" > {{release_state}}; \
	echo "TAG=$TAG" >> {{release_state}}; \
	echo "REMOTE=$remote" >> {{release_state}}

[private]
_release-bump:
	@. {{release_state}}; \
	echo "==> Bumping workspace version to $NEW_VERSION"; \
	cargo release version "$NEW_VERSION" --workspace --execute --no-confirm; \
	actual_version="$(awk '/^\[package\]/{in_pkg=1;next}/^\[/{in_pkg=0}in_pkg&&$1=="version"{v=$3;gsub(/"/,"",v);print v;exit}' orbit/Cargo.toml)"; \
	if [ "$actual_version" != "$NEW_VERSION" ]; then \
		echo "error: expected version $NEW_VERSION but found $actual_version after bump" >&2; \
		exit 1; \
	fi; \
	cargo generate-lockfile

[private]
_release-version-docs:
	@. {{release_state}}; \
	echo "==> Versioning release documentation"; \
	./scripts/version_docs.sh "$NEW_VERSION"

[private]
_release-stage:
	@echo "==> Staging release artifacts"
	@git add \
		orbit/Cargo.toml \
		orbitd/Cargo.toml \
		proto/Cargo.toml \
		Cargo.lock \
		website/docs \
		website/versioned_docs \
		website/versioned_sidebars \
		website/versions.json

[private]
_release-commit:
	@. {{release_state}}; \
	echo "==> Committing release changes"; \
	git commit -m "Release $NEW_VERSION"

[private]
_release-tag:
	@. {{release_state}}; \
	if git show-ref --tags --quiet --verify "refs/tags/$TAG"; then \
		echo "error: tag already exists: $TAG" >&2; \
		exit 1; \
	fi; \
	echo "==> Creating release tag $TAG"; \
	cargo release tag --tag-prefix "" --workspace --execute --no-confirm

[private]
_release-push:
	@. {{release_state}}; \
	echo "Release ready: version $NEW_VERSION (tag: $TAG, remote: $REMOTE)"; \
	if gum confirm --default=true "Push main and tag $TAG to $REMOTE?"; then \
		git push "$REMOTE" main; \
		git push "$REMOTE" "$TAG"; \
	else \
		echo "Skipping push."; \
	fi

clippy:
	@cargo clippy

audit:
	@cargo audit

depcheck:
	@cargo deny check licenses

local-install:
	@cargo install --path orbit
	@cargo install --path orbitd

docs-generate-cli:
	@python3 scripts/generate_cli_docs.py

docs-dev:
	@cd website && npm run start

docs-build:
	@cd website && npm run build

docs-version version:
	@./scripts/version_docs.sh {{version}}

docs-rebuild-current-version:
	@version="$(awk '/^\[package\]/{in_pkg=1;next}/^\[/{in_pkg=0}in_pkg&&$1=="version"{v=$3;gsub(/"/,"",v);print v;exit}' orbit/Cargo.toml)"; \
	echo "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || { \
		echo "error: version '$version' is not in X.Y.Z format" >&2; \
		exit 1; \
	}; \
	echo "==> Rebuilding docs version $version"; \
	rm -rf "website/versioned_docs/version-$version"; \
	rm -f "website/versioned_sidebars/version-$version-sidebars.json"; \
	if [ -f website/versions.json ]; then \
		python3 -c 'import json, sys; from pathlib import Path; p = Path("website/versions.json"); v = sys.argv[1]; data = json.loads(p.read_text()); p.write_text(json.dumps([item for item in data if item != v], indent=2) + "\\n")' "$version"; \
	fi; \
	./scripts/version_docs.sh "$version"
