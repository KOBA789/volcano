require "json"
require "benchmark"

class Index
  def initialize
    @entries = []
  end

  def bulk_load(entries)
    @entries = entries.dup.sort_by! {|(key, _)| key }
  end

  def iter(start = nil)
    return @entries.each.lazy if start.nil?
    position = @entries.bsearch_index {|(key, _)| (key <=> start) >= 0 }
    return [].each.lazy if position.nil?
    @entries[position..-1].each.lazy
  end

  def rev(start = nil)
    return @entries.reverse_each.lazy if start.nil?
    position = @entries.bsearch_index {|(key, _)| (key <=> start) <= 0 }
    return [].each.lazy if position.nil?
    @entries[0..position].reverse_each.lazy
  end
end

class SeqScan
  def initialize(
    table:,
    backward: false,
    key: nil,
    key_filter: ->(_){ true },
    filter: ->(_){ true }
  )
    @table = table
    @backward = backward
    @key = key
    @key_filter = key_filter
    @filter = filter
  end

  def iter
    table_iter
      .take_while {|key, _| @key_filter.call(key) }
      .filter     {|_, tuple| @filter.call(tuple) }
      .map        {|_, tuple| tuple }
  end

  def table_iter
    if @backward
      @table.rev(@key)
    else
      @table.iter(@key)
    end
  end
end

class Limit
  def initialize(inner:, limit:)
    @inner = inner
    @limit = limit
  end

  def iter
    @inner.iter.take(@limit)
  end
end

class IndexScan
  def initialize(
    table:,
    index:,
    index_backward: false,
    index_key: nil,
    index_filter: ->(_){ true },
    filter: ->(_){ true }
  )
    @table = table
    @index = index
    @index_backward = index_backward
    @index_key = index_key
    @index_filter = index_filter
    @filter = filter
  end

  def iter
    index_iter
      .take_while {|index_key, _| @index_filter.call(index_key) }
      .map        {|_, primary_key| @table.iter(primary_key).first }
      .filter     {|_, tuple| @filter.call(tuple) }
      .map        {|_, tuple| tuple }
  end

  def index_iter
    if @index_backward
      @index.rev(@index_key)
    else
      @index.iter(@index_key)
    end
  end
end

class Sort
  def initialize(inner:, columns:)
    @inner = inner
    @columns = columns
  end

  def iter
    all_tuples = @inner.iter.force
    all_tuples.sort! do |a, b|
      @columns
        .lazy
        .map    {|name, order| (a[name] <=> b[name]) * order }
        .reject {|x| x == 0 }
        .first
    end
    all_tuples.lazy
  end
end

puts "Loading..."

def load_data(file)
  rows = []
  File.open(file, "r") do |f|
    f.each_line do |json|
      rows << JSON.parse(json, symbolize_names: true)
    end
  end
  rows
end

user_rows = load_data("data/users.json")
item_rows = load_data("data/items.json")

users = Index.new
users_account_name_idx = Index.new
users.bulk_load(
  user_rows.map {|row| [row[:id], row]}
)
users_account_name_idx.bulk_load(
  user_rows.map {|row| [row[:account_name], row[:id]]}
)

items = Index.new
items_buyer_id_idx = Index.new
items_created_at_idx = Index.new
items.bulk_load(
  item_rows.map {|row| [row[:id], row]}
)
items_buyer_id_idx.bulk_load(
  item_rows.map {|row| [[row[:buyer_id], row[:id]], row[:id]]}
)
items_created_at_idx.bulk_load(
  item_rows.map {|row| [[row[:created_at], row[:id]], row[:id]]}
)

Benchmark::bm(40) do |x|
  puts "SELECT * FROM items WHERE buyer_id = 1;"
  seq_scan = nil
  index_scan = nil
  x.report("SeqScan") do
    plan = SeqScan.new(table: items, filter: ->(tuple){tuple[:buyer_id] == 1})
    seq_scan = plan.iter.force
  end
  x.report("IndexScan") do
    plan = IndexScan.new(
      table: items,
      index: items_buyer_id_idx,
      index_key: [1],
      index_filter: ->((buyer_id, _pk)){ buyer_id == 1 }
    )
    index_scan = plan.iter.force
  end
  raise unless seq_scan == index_scan

  puts "SELECT * FROM items ORDER BY created_at DESC, id DESC;"
  seq_scan_sort_limit = nil
  index_scan_limit = nil
  x.report("SeqScan + Sort + Limit") do
    plan = Limit.new(
      inner: Sort.new(
        inner: SeqScan.new(table: items),
        columns: { created_at: -1, id: -1 },
      ),
      limit: 10,
    )
    seq_scan_sort_limit = plan.iter.force
  end

  x.report("IndexScan + Limit") do
    plan = Limit.new(
      inner: IndexScan.new(
        table: items,
        index: items_created_at_idx,
        index_backward: true,
      ),
      limit: 10,
    )
    index_scan_limit = plan.iter.force
  end
  raise unless seq_scan_sort_limit == index_scan_limit
end
