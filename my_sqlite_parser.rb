require 'csv'
require "readline"
#   sqlParser
#
#   parser class to parse an sql request in string form and hold the usefull contents
#
#   @param {String} request
#
class SQLParser
    def initialize(request)
        @s = StringScanner.new(request)
        @table_name = ''
        @data = Hash.new
        @flags = Hash.new
        @instr = ''
        @error = 0
        @errorDesc = [
            'no error',
            'ERROR: missing INTO after Insert',
            'ERROR: no FROM keyword',
            'ERROR: missing VALUES keyword',
            'required SQL keyword not found',
            'Missing keyword in sql request',
        ]
    end
    def getData()
        return @data
    end
    def getFlags()
        return @flags
    end
    def getTableName()
        return @table_name
    end
    def getInstruction()
        return @instr
    end
    def setError(err_num)
        @error = err_num
    end
    def getError()
        return @error
    end
    def getErrorMsg()
        return @errorDesc[@error]
    end
    def getSelectData()
        return @data['select']
    end
    def getJoinTableName()
        return @data['join_table']
    end
    def getJoinCol()
        return @join_col
    end
    def getTableCol()
        return @table_col
    end
    def getOrderCol()
        if @data['order'][0] == nil
            return nil
        else
            return @data['order'][0]
        end  
    end
    def getOrder()
        if @data['order'][1] == nil
            return nil
        else
            return @data['order'][1]
        end
    end
    def getWhereColCriteria()
        if @data['where'] == nil
            return nil
        else
            return @data['where'][1]
        end
    end
    def getWhereColName()
        if @data['where'] == nil
            return nil
        else
            return @data['where'][0]
        end
    end
    def getValues()
        if @data['values'].empty?
            return nil
        else
            return @data['values']
        end
    end
    #   sqlParser
    #
    #   parses instruction then if no error's occured parses that instruction
    #
    #   @param {String} request
    #   @return {Int} 0 on error, 1 on success
    def Parse()
        self.parseInstr()
        if getError > 0
            puts "error parsing instruction "
            return -1
        end
        ret = 0
        case @instr
        when 'SELECT'
            ret = self.parseSelect()
        when 'DELETE'
            ret = self.parseDelete()
        when 'INSERT'
            ret = self.parseInsert()
        when 'UPDATE'
            ret = self.parseUpdate()
        else
            return 0
        end
        @error = ret
        setError(ret)
    end
    def parseInstr()
        instr = @s.scan(/(\w+)/)
        if instr == 'INSERT'
            k = @s.scan(/ (\w+)/)
            k = k.lstrip.rstrip
            if k != 'INTO'
                setError(1)
            end
        elsif instr == 'DELETE'
            k = @s.scan(/ (\w+)/)
            k = k.lstrip.rstrip
            if k != 'FROM'
                setError(2)
            end
        end
        @instr = instr
    end
    def setFlags(flagNames)
        flagNames.each do |flag|
            if @s.exist?(/#{flag}/)
                @flags[flag] = 1
            else
                @flags[flag] = 0
            end
        end
    end
    def parseEquality(valuesStr)
        valuesArr = valuesStr.split('=')
        valuesArr = valuesArr.map do |value|
            value = value.lstrip()
            value = value.rstrip()
        end
        return valuesArr
    end 
#   parseSelect
#
#   parse a SELECT request and fills the Parser table_name and @data with the WHERE and ORDER_BY content if applicable
#
#   @return {Number} returns 0 on success and an error number on failure
#
    def parseSelect()
        setFlags(['FROM', 'WHERE', 'ORDER BY', 'JOIN'])
        if @flags['FROM'] == 0
            return 2
        end
        #parse select column values
        select_values = @s.scan_until(/FROM/)
        select_values = select_values[0, select_values.length - 4].lstrip.rstrip
        @data['select'] = select_values.split(', ')
        #parse table Name
        name = @s.scan_until(/(\w+)/)
        if name != nil
            @table_name = name.lstrip.rstrip
        end
        #check for JOIN
        if @flags['JOIN'] == 1
            self.parseJoin
            return 0
        end
        #parse Where if exists
        if @flags['WHERE'] == 1
            table = @s.scan_until(/WHERE/)
            if @flags['ORDER BY'] == 1
                where_values = @s.scan_until(/ORDER BY/)
                where_values = where_values[0, where_values.length - 5].lstrip.rstrip
                @data['where'] = self.parseEquality(where_values)
                @data['order'] = @s.rest.lstrip.rstrip.split(' ')
            else
                @data['where'] = self.parseEquality(@s.rest.lstrip.rstrip)
            end
        end
        #parse ORDER_BY if exists when WHERE does not
        if @flags['ORDER BY'] == 1
            table = @s.scan_until(/ORDER BY/)
            @data['order'] = @s.rest.lstrip.rstrip.split(' ')
        end
        return 0
    end
    def sanitizeTableName()
        if @table_name != nil && !@table_name.include?(".csv")
            @table_name = @table_name + ".csv"
        end
    end
    def parseJoin()
        join_table = @s.scan_until(/JOIN/)
        join_table = @s.scan_until(/ON/)
        if join_table == nil
            return 1
        end
        @data['join_table'] = join_table[0, join_table.length - 3].lstrip.rstrip
        eqVals = self.parseEquality(@s.rest.lstrip.rstrip)
        eqVals.each do |v|
            table_col = v.split('.')
            if table_col[0] == @data['join_table']
                @join_col = table_col[1]
            elsif table_col[0] == @table_name
                @table_col = table_col[1]
            end
            
        end
        if @data['join_table'] != nil && !@data['join_table'].include?(".csv")
            @data['join_table'] = @data['join_table'] + ".csv"
        end
    end
    def parseDelete()
        setFlags(['FROM', 'WHERE'])
        return 1
    end
    def parseInsert()
        setFlags(['VALUES'])
        if @flags['VALUES'] == 0
            return 3
        end
        table = @s.scan_until(/\(/).chomp
        @table_name = table.chomp('(').lstrip.rstrip
        insideParens = @s.scan_until(/\)/)
        insColmns = insideParens.chomp(')').split(',')
        if insColmns != nil
            insColmns = insColmns.map do |value|
              value = value.lstrip().rstrip
            end
        else
            insColmns = Array.new
            insColmns.push(insideParens)
        end
        @s.scan_until(/\(/)
        parenValues = @s.scan_until(/\)/)
        if parenValues == nil
            return 4
        end
        values = parenValues.chomp(')').split(',')
        if values != nil
            values = values.map do |value|
              value.lstrip.rstrip
            end
        else
            values = Array.new
            values.push(parenValues)
        end
        @data['values'] = Hash.new
        insColmns.each_index do |i|
            @data['values'][insColmns[i]] = values[i]
        end
        return 0
    end
    def parseUpdate()
        setFlags(['SET', 'WHERE'])
        if @flags['SET'] == 0 || @flags['WHERE'] == 0
            return 5
        end
        table = @s.scan_until(/SET/)
        @table_name = table[0, table.length - 4].lstrip.rstrip        
        changes = @s.scan_until(/WHERE/)
        changes = changes[0, changes.length - 6].lstrip.rstrip
        pt = changes.split(',')
        @data['values'] = Hash.new
        pt.each do |eq|
            hashVals = self.parseEquality(eq)
            @data['values'][hashVals[0]] = hashVals[1]
        end
        where = @s.rest
        @data['where'] = self.parseEquality(@s.rest.lstrip.rstrip)
        return 0
    end
    def parseDelete()
        setFlags(['WHERE'])
        if @flags['WHERE'] == 0
            @table_name = @s.rest.lstrip.rstrip
        else
            table = @s.scan_until(/WHERE/)
            @table_name = table[0, table.length - 6].lstrip.rstrip
            @data['where'] = self.parseEquality(@s.rest.lstrip.rstrip)
        end
        return 0
    end
    def parseEquality(valuesStr)
        valuesArr = valuesStr.split('=')
        valuesArr = valuesArr.map do |value|
            value = value.lstrip()
            value = value.rstrip()
        end
        return valuesArr
    end
end
