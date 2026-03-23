# Charts Module

Independent module for custom chart development.

## Goals

- keep chart UI independent from old scanner/runtime tabs
- keep chart-specific logic inside `app/charts`
- build a clean base for custom spread charts

## Current structure

- `models.py` - basic point and candle models
- `ui/price_chart_widget.py` - custom chart surface
- `ui/price_chart_window.py` - standalone chart window shell
- `history/` - groundwork for historical price loading and spread series building

## Rule

Copy only neutral infrastructure into this module.
Do not pull old business logic into `charts`.
