defmodule Ecto.Adapters.Mnesia do
  @behaviour Ecto.Adapter.Storage
  
  def storage_up(opts) do
    username = Keyword.fetch!(opts, :username)
    host = Keyword.fetch!(opts, :hostname)
    hostname = String.to_atom("#{username}@#{host}")
    :net_kernel.start([hostname, :shortnames])

    schema = Keyword.get(opts, :disc_nodes, [Node.self])
    if dir = Keyword.get(opts, :directory) do
      :application.set_env(:mnesia, :dir, dir)
    end
    :mnesia.stop
    :mnesia.create_schema(schema)
  end

  def storage_down(opts) do
    username = Keyword.fetch!(opts, :username)
    host = Keyword.fetch!(opts, :hostname)
    hostname = String.to_atom("#{username}@#{host}")
    :net_kernel.start([hostname, :shortnames])
    
    schema = Keyword.get(opts, :disc_nodes, [Node.self])
    if dir = Keyword.get(opts, :directory) do
      :application.set_env(:mnesia, :dir, dir)
    end
    :mnesia.stop
    :mnesia.delete_schema(schema)
  end


  #Connection behaviours 

  def __using__(adapter) do
    quote do  
      @behaviour Ecto.Adapter
      @behaviour Ecto.Adapter.Migration
      @behaviour Ecto.Adapter.Transaction 
    
      defmacro __before_compile__(env) do

      end

      def start_link(repo, opts) do
        Ecto.Adapters.Mnesia.start_link(repo, opts)
      end

      def stop(repo) do
        Ecto.Adapters.Mnesia.stop(repo)
      end
 
      def execute_dll(dll) do
        Ecto.Adapters.Mnesia.execute_dll(dll)
      end

    end
  end

  def __before_compile__(env) do

  end

  def transaction(repo, options, fun) do
    case :mnesia.transaction(fun) do
      {:aborted, reason} ->
        {:error, reason}
      {:atomic, result} ->
        {:ok, result}
    end
  end

  def rollback(repo, reason) do
    :mnesia.abort(reason)
  end


  def start_link(repo, options) do
    GenServer.start_link(__MODULE__, [repo, options])
  end

  def init([repo, options]) do
    :mnesia.start
    {:ok, [repo, options]}
  end



  def stop(repo) do
    :application.stop(:mnesia)
  end

  ## Query
  alias Ecto.Query.SelectExpr
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.JoinExpr
  def all(repo, query, opt1, opt2) do
    sources = create_names(query)
  
    []
  end
 

  defp from(sources) do
    {table, _name, _model} = elem(sources, 0)
    table = :mnesia.table(String.to_atom(table))
  end

  defp select(%SelectExpr{fields: fields}, distinct, sources) do
    distinct(distinct, sources) 
    fields
  end
  
  defp distinct(nil, _sources), do: false
  defp distinct(%QueryExpr{expr: true}, _sources), do: true
  defp distinct(%QueryExpr{expr: false}, _sources), do: false
  #defp distinct(%QueryExpr{expr: exprs}, sources) when is_list(exprs) do
    #"DISTINCT ON (" <> Enum.map_join(exprs, ", ", &expr(&1, sources)) <> ") "
  #end


  defp create_names(query) do
    sources = query.sources |> Tuple.to_list
    Enum.reduce(sources, [], fn {table, model}, names ->
      name = unique_name(names, String.first(table), 0)
      [{table, name, model}|names]
    end) |> Enum.reverse
  end
  
  # Brute force find unique name
  defp unique_name(names, name, counter) do
    counted_name = name <> Integer.to_string(counter)
      if Enum.any?(names, fn {_, n, _} -> n == counted_name end) do
        unique_name(names, name, counter + 1)
      else
        counted_name
      end
  end
   
  # DDL

  alias Ecto.Migration.Table
  alias Ecto.Migration.Index
  alias Ecto.Migration.Reference

  def supports_ddl_transaction?, do: false

  def ddl_exists?(_repo, %Table{name: name}, _opts) do
    case :mnesia.transaction(fn->
      :mnesia.table_info(name, :all) 
    end) do
      {:aborted, {:no_exists, _, _}} -> false
      _ -> true
    end
  end

  def execute_ddl(_repo, {:create, %Table{name: name}, columns}, opts) do
    :mnesia.create_table(name, create_attributes(columns))
    |> atomic_response
  end

  def execute_ddl({:drop, %Table{name: name}}) do
    :mnesia.delete_table(name)
    |> atomic_response
  end

  defp create_attributes(columns) do
    attributes = []
  
    Enum.each(columns, fn {:add, column, _type, opts} when is_atom(column)->
        if Keyword.get(opts, :primary_key) do
          List.insert_at(attributes, column, 0)
        else
          List.insert_at(attributes, column, -1)
        end
    end)
    [attributes: attributes]
  end

  defp atomic_response({:atomic, result}), do: result
  
  defp atomic_response({:aborted, reason}), do: reason

end


defmodule Mnesia.Test do
  def test do
    table1 = :mnesia.table(:a)
    table2 = :mnesia.table(:b)
    :qlc_bridge.join(table1, table2, fn(a,b)-> elem(a,2) == elem(b,2) end)
    |> :qlc_bridge.select(
        fn(sel) -> sel end, 
        fn(sel)-> true end,
        false)
    |> :qlc_bridge.sort(2, :descending)
    |> :qlc_bridge.offset(1)
    |> :qlc_bridge.limit(3)
  end
  def populate do
    for i <- 1..20
        do
          :mnesia.dirty_write(:a, {:a, i, i+1, i+2})
          :mnesia.dirty_write(:b, {:b, i, i+1, i+2})
        end
  end
 
end

