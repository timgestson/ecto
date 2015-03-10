-module(qlc_bridge).
-export([join/3, select/4, sort/3, offset/2, limit/2, count/1, distinct/2, distinct/1,
	aggregate/3]).
-include_lib("stdlib/include/qlc.hrl").

join(Table1, Table2, CondFunc)->
	qlc:q([
		list_to_tuple(lists:append(tuple_to_list(A),tuple_to_list(B)))
		|| A <- Table1
		,  B <- Table2
		, CondFunc(A,B)
	       ]).

select(Table, SelectFunc, CondFunc, Distinct) ->
	qlc:q([
		SelectFunc(A)
		|| A <- Table
		, CondFunc(A)
		], {unique, Distinct}).

sort(Table, Key, Order) ->
	qlc:keysort(Key, Table, [{order, Order}]).

offset(Table, Offset) ->
	Curser = qlc:cursor(Table),
	case qlc:next_answers(Curser, Offset) of
		{error, Error} ->
			{error, Error};
		_->
			Curser
	end.

limit(Curser, Limit) ->
	Answers = qlc:next_answers(Curser, Limit),
	qlc:delete_cursor(Curser),
	Answers.

count(Table) ->
	qlc:fold(fun(_row, acc)->
		acc + 1
	end, 0, Table).

aggregate(Table, Fun, Acc) ->
	qlc:fold(Fun, Acc, Table).

distinct(Table, Key)->
	qlc:q([ element(Key, Out) || Out <-Table], {unique, true}).

distinct(Table) ->
	qlc:q([Out || Out<-Table], {unique, true}).

