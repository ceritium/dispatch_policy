# frozen_string_literal: true

module DispatchPolicy
  module ApplicationHelper
    # Inline SVG sparkline from an array of numeric values.
    def sparkline(values, width: 80, height: 20, stroke: "#0057b7")
      return content_tag(:span, "—", class: "muted") if values.nil? || values.empty?

      vmin = values.min.to_f
      vmax = values.max.to_f
      span = vmax - vmin
      span = 1.0 if span.zero?

      step = values.size > 1 ? (width.to_f / (values.size - 1)) : 0
      points = values.each_with_index.map { |v, i|
        x = (i * step).round(1)
        y = (height - (v - vmin) / span * height).round(1)
        "#{x},#{y}"
      }.join(" ")

      content_tag(:svg, width: width, height: height, viewBox: "0 0 #{width} #{height}",
                        xmlns: "http://www.w3.org/2000/svg", style: "vertical-align: middle;") do
        tag.polyline(points: points, fill: "none", stroke: stroke, "stroke-width": 1.5)
      end
    end

    # Inline SVG line chart for [[Time, value], ...] series. Renders a
    # baseline + a single polyline + y-axis hint text.
    def line_chart_svg(series, width: 700, height: 120, stroke: "#0057b7", label: nil)
      return content_tag(:p, "No samples yet.", class: "muted") if series.nil? || series.empty?

      values = series.map { |(_, v)| v.to_f }
      times  = series.map(&:first)
      vmin   = values.min
      vmax   = values.max
      span   = vmax - vmin
      span   = 1.0 if span.zero?

      pad_x  = 40
      pad_y  = 15
      plot_w = width - 2 * pad_x
      plot_h = height - 2 * pad_y
      step   = values.size > 1 ? (plot_w.to_f / (values.size - 1)) : 0

      points = values.each_with_index.map { |v, i|
        x = (pad_x + i * step).round(1)
        y = (pad_y + plot_h - (v - vmin) / span * plot_h).round(1)
        "#{x},#{y}"
      }.join(" ")

      content_tag(:svg, width: width, height: height, viewBox: "0 0 #{width} #{height}",
                        xmlns: "http://www.w3.org/2000/svg", style: "display: block;") do
        concat(tag.rect(x: 0, y: 0, width: width, height: height, fill: "#fafafa", stroke: "#eee"))
        concat(tag.polyline(points: points, fill: "none", stroke: stroke, "stroke-width": 1.8))
        concat(content_tag(:text, "#{vmax.round(1)}ms",
                           x: 4, y: pad_y + 4, "font-size": 10, fill: "#666"))
        concat(content_tag(:text, "#{vmin.round(1)}ms",
                           x: 4, y: height - pad_y + 4, "font-size": 10, fill: "#666"))
        concat(content_tag(:text, times.first.strftime("%H:%M"),
                           x: pad_x, y: height - 3, "font-size": 10, fill: "#666"))
        concat(content_tag(:text, times.last.strftime("%H:%M"),
                           x: width - pad_x - 26, y: height - 3, "font-size": 10, fill: "#666"))
        concat(content_tag(:text, label, x: width / 2 - 40, y: 12, "font-size": 11, fill: "#444")) if label
      end
    end
  end
end
