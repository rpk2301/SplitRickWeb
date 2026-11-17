// OG card source for Ricasso game invites (splitrick.click/play/<code>): renders the
// 1200×630 PNG used as og:image on public/play.html, in Ricasso's own look (dotted paper,
// ink-outlined card, rounded heavy type, the self-drawn snowman). Not deployed; kept here so
// the design is version-controlled. From the repo root:
//
//   swift og-ricasso.swift
//
// writes public/og-ricasso-2.png.

import AppKit
import SwiftUI

extension Color {
    init(_ hex: UInt32) {
        self.init(red: Double((hex >> 16) & 0xFF) / 255, green: Double((hex >> 8) & 0xFF) / 255,
                  blue: Double(hex & 0xFF) / 255)
    }
}
let paper = Color(0xF6F3EC), dot = Color(0xD9D3C4), ink = Color(0x17161A), muted = Color(0x4A4852)
let orange = Color(0xFF5A36), yellow = Color(0xFFD23F), blue = Color(0x2F6BFF), sky = Color(0x8FB3FF)
let brown = Color(0x8B5A2B)

/// The snowman from the app's sample drawings, in 0...1 coordinates.
struct Snowman: View {
    var body: some View {
        Canvas { ctx, size in
            let s = size.width
            func p(_ x: Double, _ y: Double) -> CGPoint { CGPoint(x: x * s, y: y * s) }
            func circle(_ cx: Double, _ cy: Double, _ r: Double) -> Path {
                Path(ellipseIn: CGRect(x: (cx - r) * s, y: (cy - r) * s, width: 2 * r * s, height: 2 * r * s))
            }
            func line(_ pts: [CGPoint], _ color: Color, _ w: Double) {
                var path = Path(); path.addLines(pts)
                ctx.stroke(path, with: .color(color), style: StrokeStyle(lineWidth: w * s, lineCap: .round, lineJoin: .round))
            }
            line([p(0.08, 0.95), p(0.5, 0.92), p(0.92, 0.95)], sky, 0.014)
            for (cy, r) in [(0.76, 0.168), (0.5, 0.123), (0.3, 0.089)] {
                ctx.stroke(circle(0.5, cy, r), with: .color(ink), lineWidth: 0.014 * s)
            }
            line([p(0.39, 0.215), p(0.61, 0.21)], ink, 0.02)
            ctx.fill(Path(CGRect(x: 0.44 * s, y: 0.09 * s, width: 0.12 * s, height: 0.115 * s)), with: .color(ink))
            ctx.fill(circle(0.465, 0.285, 0.011), with: .color(ink))
            ctx.fill(circle(0.535, 0.28, 0.011), with: .color(ink))
            line([p(0.5, 0.315), p(0.6, 0.33), p(0.5, 0.335)], orange, 0.016)
            line([p(0.41, 0.385), p(0.5, 0.41), p(0.59, 0.38)], blue, 0.03)
            line([p(0.38, 0.49), p(0.24, 0.4)], brown, 0.014)
            line([p(0.28, 0.43), p(0.25, 0.37)], brown, 0.012)
            line([p(0.62, 0.49), p(0.76, 0.41)], brown, 0.014)
            line([p(0.72, 0.43), p(0.76, 0.37)], brown, 0.012)
            for y in [0.46, 0.52, 0.58] { ctx.fill(circle(0.5, y, 0.01), with: .color(ink)) }
        }
    }
}

struct Squiggle: Shape {
    func path(in r: CGRect) -> Path {
        var p = Path()
        p.move(to: CGPoint(x: r.minX, y: r.midY))
        p.addCurve(to: CGPoint(x: r.midX, y: r.midY), control1: CGPoint(x: r.width * 0.15, y: r.minY), control2: CGPoint(x: r.width * 0.35, y: r.maxY))
        p.addCurve(to: CGPoint(x: r.maxX, y: r.midY), control1: CGPoint(x: r.width * 0.65, y: r.minY), control2: CGPoint(x: r.width * 0.85, y: r.maxY))
        return p
    }
}

struct Card: View {
    var body: some View {
        ZStack {
            Canvas { ctx, size in
                var y = 12.0
                while y < size.height {
                    var x = 12.0
                    while x < size.width {
                        ctx.fill(Path(ellipseIn: CGRect(x: x - 2, y: y - 2, width: 4, height: 4)), with: .color(dot))
                        x += 30
                    }
                    y += 30
                }
            }
            .background(paper)

            HStack(spacing: 70) {
                Snowman()
                    .frame(width: 330, height: 330)
                    .background(RoundedRectangle(cornerRadius: 34, style: .continuous).fill(.white))
                    .overlay(RoundedRectangle(cornerRadius: 34, style: .continuous).strokeBorder(ink, lineWidth: 6))
                    .background(RoundedRectangle(cornerRadius: 34, style: .continuous).fill(ink).offset(x: 10, y: 14))
                    .overlay(alignment: .top) {
                        Rectangle().fill(yellow.opacity(0.9)).frame(width: 110, height: 34)
                            .rotationEffect(.degrees(-4)).offset(y: -17)
                    }
                    .rotationEffect(.degrees(-4))

                VStack(alignment: .leading, spacing: 18) {
                    Text("Come draw\nwith me!")
                        .font(.system(size: 92, weight: .heavy, design: .rounded))
                        .tracking(-2)
                        .lineSpacing(-6)
                        .foregroundStyle(ink)
                    Text("Tap to join my Ricasso game")
                        .font(.system(size: 34, weight: .semibold, design: .rounded))
                        .foregroundStyle(muted)
                }
            }
            .padding(.horizontal, 90)
        }
        .frame(width: 1200, height: 630)
    }
}

@MainActor func render() {
    let renderer = ImageRenderer(content: Card())
    renderer.scale = 1
    guard let image = renderer.nsImage, let tiff = image.tiffRepresentation,
          let rep = NSBitmapImageRep(data: tiff), let png = rep.representation(using: .png, properties: [:]) else {
        fatalError("render failed")
    }
    try! png.write(to: URL(fileURLWithPath: "public/og-ricasso-2.png"))
    print("wrote public/og-ricasso-2.png", rep.pixelsWide, "x", rep.pixelsHigh)
}
MainActor.assumeIsolated { render() }
