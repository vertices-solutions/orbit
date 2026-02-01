set shell := ["bash", "-cu"]

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
	@./scripts/release.sh
clippy:
	@cargo clippy
audit:
	@cargo audit
depcheck:
	@cargo deny check licenses