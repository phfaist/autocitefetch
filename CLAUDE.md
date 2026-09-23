# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Rust workspace that retrieves bibliographic citations from multiple sources
(arXiv, doi.org, local bib files, manual entries) into CSL-JSON. The core crate
is `#![no_std]` + `alloc` and executor-agnostic so the *same* code runs on a
native `std` host and in a browser (WASM). It is a port of two prior libraries
(see "Reference implementations" below).

Additional information: `README.md`.

## Commands

- Run tests: `cargo test`

- Build docs: `cargo doc --workspace --no-deps`

- Clippy: `cargo clippy --workspace --all-targets`

- Build on WASM: `cargo build -p autocitefetch --target wasm32-unknown-unknown`

- Build for std without ureq/TLS: `cargo build -p autocitefetch-std --no-default-features`

- Do NOT use `cargo fmt`.

## Architecture

Architecture information in `ARCH.md`.

