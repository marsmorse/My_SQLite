require_relative 'my_sqlite_parser.rb'
require_relative 'my_sqlite_request.rb'
require_relative 'validator.rb'
require 'csv'
require "readline"
def runSelect(sql_parser)
    request = MySqliteRequest.new
    keyword_flags = sql_parser.getFlags()
    validator = Validator.new
    if validator.validTableName(sql_parser.getTableName)
        request = request.from(sql_parser.getTableName)
        s_data = sql_parser.getSelectData        
        if !validator.validSelectValues(s_data)#!###
            return 0
        end
        if keyword_flags['JOIN'] == 1 #JOIN Request
            request = request.select(s_data)
            request = request.join(sql_parser.getTableCol, sql_parser.getJoinTableName, sql_parser.getJoinCol)
        elsif keyword_flags['WHERE'] == 1 && keyword_flags['ORDER BY'] == 1#Select with ORDER and WHERE
            if !validator.validWhere(sql_parser.getWhereColName, sql_parser.getWhereColVal) || !validator.validOrder(sql_parser.getOrderCol, sql_parser.getOrder)
                return 0
            end
            request = request.where(sql_parser.getWhereColName, sql_parser.getWhereColCriteria)
            request = request.setOrder(sql_parser.getOrder, sql_parser.getOrderCol) 
            request = request.select(s_data)
        elsif keyword_flags['WHERE'] == 1
            if !validator.validWhere(sql_parser.getWhereColName, sql_parser.getWhereColCriteria) 
                return 0
            end
            request = request.where(sql_parser.getWhereColName, sql_parser.getWhereColCriteria)
            request = request.select(s_data)
        elsif keyword_flags['ORDER BY'] == 1
            if !validator.validOrder(sql_parser.getOrderCol, sql_parser.getOrder)
                return 0
            end
            request = request.order(sql_parser.getOrder, sql_parser.getOrderCol) 
            request = request.select(s_data)
        else
            request = request.select(s_data)
        end
    else
        return 0
    end
    return request.run
end
def runInsert(sql_parser)
    request = MySqliteRequest.new
    keyword_flags = sql_parser.getFlags()
    validator = Validator.new
    if validator.validTableName(sql_parser.getTableName)
        request = request.insert(sql_parser.getTableName)
        request = request.values(sql_parser.getValues)
    else
        return 0
    end
    if request != nil
        return request.run
    end
    return 0
end
def runUpdate(sql_parser)
    request = MySqliteRequest.new
    keyword_flags = sql_parser.getFlags()
    validator = Validator.new
    if validator.validTableName(sql_parser.getTableName)
        request = request.update(sql_parser.getTableName)
        if keyword_flags["WHERE"] = 1 && validator.validWhere(sql_parser.getWhereColName, sql_parser.getWhereColCriteria) 
            request = request.where(sql_parser.getWhereColName, sql_parser.getWhereColCriteria)
            request = request.values(sql_parser.getValues)
        else
            return 0
        end 
    else
        return 0
    end
    if request != nil
        return request.run
    end
    return 0
end
def runDelete(sql_parser)
    request = MySqliteRequest.new
    keyword_flags = sql_parser.getFlags()
    validator = Validator.new
    if validator.validTableName(sql_parser.getTableName)
        request = request.delete(sql_parser.getTableName)
        if keyword_flags["WHERE"] = 1 && validator.validWhere(sql_parser.getWhereColName, sql_parser.getWhereColCriteria) 
            request = request.where(sql_parser.getWhereColName, sql_parser.getWhereColCriteria)
        end 
    else
        return 0
    end
    if request != nil
        return request.run
    end
    return 0
end
while buf = Readline.readline("my_sqlite> ", true)
    if buf[-1] == ';'
        h = buf[0, buf.length - 2]
        input_arr = Readline::HISTORY.to_a
        input_arr[-1] = input_arr[-1].chop
        requestString = input_arr.join(' ')
        if  requestString == nil
            puts "Error: empty request"
        else
            sql_parser = SQLParser.new(requestString)
            sql_parser.Parse()
            sql_parser.sanitizeTableName()
            if sql_parser.getError() > 0
                puts sql_parser.getErrorMsg()
            else
                if sql_parser.getInstruction == 'SELECT'
                    result = runSelect(sql_parser)
                elsif sql_parser.getInstruction == 'INSERT'
                    result = runInsert(sql_parser)
                elsif sql_parser.getInstruction == 'UPDATE'
                    result = runUpdate(sql_parser)
                elsif sql_parser.getInstruction == 'DELETE'
                    result = runDelete(sql_parser)
                else
                    puts "Error: Could invalid SQL instruction:'#{sql_parser.getInstruction}' "
                end
                if result != nil
                    puts result
                end
            end

        end
        Readline::HISTORY.clear
    end
end
#data = Hash.new
#data['name'] = "Obi Wan"
#data["college"] = "UCSC"
#request = MySqliteRequest.new
=begin
#JOIN TEST
request = request.from("nba_players.csv")
request = request.select(['Pid', 'Player', 'height', 'weight', 'college', ''])
request = request.join("Player", "Seasons_Stats.csv", "Player")
puts request.run
=end
=begin
#ORDER BY
request = MySqliteRequest.new
request = request.from("nba_player_data.csv")
request = request.where('year_start', '1991')
request = request.setOrder(":desc", "name")
request = request.select(['name', 'position', 'weight', 'college'])
puts request.run
=end
=begin
#DELETE
request = request.delete("nba_player_data.csv")
request = request.where('name', 'Obi Wan')
=end
#puts request.run
=begin
#INSERT
request = request.insert('nba_player_data.csv')
request = request.values(data)
request.run
=end
