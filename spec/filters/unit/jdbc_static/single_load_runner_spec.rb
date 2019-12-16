# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require_relative "../../helpers/shared_helpers"

require "logstash/filters/jdbc_static/single_load_runner"

describe LogStash::Filters::JdbcStatic::SingleLoadRunner  do
  let(:local_db) { double("local_db") }
  let(:loaders) { Object.new }
  let(:local_db_objects) { [] }
  subject(:runner) { described_class.new(local_db, loaders, local_db_objects) }

  it_behaves_like "a single load runner"
end
