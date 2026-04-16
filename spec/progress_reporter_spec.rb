# frozen_string_literal: true

RSpec.describe Tickrake::ProgressReporter do
  it "writes line-based progress updates for non-tty outputs" do
    output = StringIO.new

    reporter = described_class.build(total: 3, title: "SPX 30min", output: output)
    reporter.advance(title: "SPX 30min chunk 1/3")
    reporter.advance(title: "SPX 30min chunk 2/3")
    reporter.advance(title: "SPX 30min chunk 3/3")
    reporter.finish

    expect(output.string).to include("SPX 30min chunk 1/3 (1/3)")
    expect(output.string).to include("SPX 30min chunk 2/3 (2/3)")
    expect(output.string).to include("SPX 30min chunk 3/3 (3/3)")
  end

  it "uses ruby-progressbar for tty outputs" do
    output = StringIO.new
    allow(output).to receive(:tty?).and_return(true)
    progressbar = double("progressbar", total: 1, finished?: false, increment: true, finish: true)
    allow(progressbar).to receive(:total=)
    allow(progressbar).to receive(:title=)

    allow(ProgressBar).to receive(:create).with(
      total: 1,
      title: "SPY day",
      output: output,
      autofinish: false
    ).and_return(progressbar)

    reporter = described_class.build(total: 1, title: "SPY day", output: output)
    reporter.advance
    reporter.add_total(1)
    reporter.finish

    expect(ProgressBar).to have_received(:create)
    expect(progressbar).to have_received(:increment)
    expect(progressbar).to have_received(:finish)
  end
end
