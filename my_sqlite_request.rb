#request = MySqliteRequest.new
#request = request.from('nba_player_data.csv')
#request = request.select('name')
#request = request.where('birth_state', 'Indiana')
#request.run
#=> [{"name" => "Andre Brown"]

#Input: MySqliteRequest('nba_player_data').select('name').where('birth_state', 'Indiana').run
#Output: [{"name" => "Andre Brown"]
require 'csv'
class MySqliteRequest
    #constructor functions
    def initialize
        @table_name = ''
        @table = nil
        @join_table = nil
        @operation = ''
        @where = ''
        #array containing columns to display for select and join operations
        @columns = []
        #array with 2 columns to join on. the select database specified with FROM is in index 0 and the second db specified in the JOIN is index 1
        @join_columns = []
        @result = []
        @filter = Hash.new
        @join = ''
        @data = nil
        @order = 'nill'
        @join_column_hash = Hash.new
        @order_column = ''        
        self
    end

#helper functions that print class vars for debugging purposes
    def getTable() 
        puts @table[0]["name"];
    end
    def getColumns()
        puts @columns
    end
    def getFilter()
        puts @filter
    end
    def getJoinColumnA()
        return @join_columns[0]
    end
    def getJoinColumnB() 
        return @join_columns[1]
    end
    def checkIfColumnsInTable()
        CSV.foreach(@table_name, 'r', headers:false) do |row|
            count = 0
            row.each do |header|
                @columns.each do |element|
                     if header == element
                        count+=1
                        break
                    end
                end
                if count == @columns.length
                    return true
                end
            end
            return false
        end
    end
#Core functions
    def from(table_name)
        @table = CSV.read(table_name, headers: true)
        @table_name = table_name
        self
    end
    def where(column_name, criteria)
        @filter['column_name'] = column_name
        @filter['criteria'] = criteria
        self
    end
#Order functions
    #returns true if left element should come before right element false if opposite
    def cmp (left_element, right_element)
        #puts left_element
        #puts right_element
        if @order == "ASC"
            if left_element[@order_column] < right_element[@order_column]
                return true
            else
                return false
            end
        else
            if left_element[@order_column] > right_element[@order_column]
                return true
            else
                return false      
            end    
        end
    end
    def merge(left_arr, right_arr)
        return_arr = []
        right_it = 0
        left_it = 0
        until left_it >= left_arr.length || right_it >= right_arr.length
            if self.cmp(left_arr[left_it], right_arr[right_it])
                return_arr.push(left_arr[left_it])
                left_it += 1
            else   
                return_arr.push(right_arr[right_it])
                right_it += 1
            end
        end
        if left_it >= left_arr.length
            until right_it >= right_arr.length do
                return_arr.push(right_arr[right_it])
                right_it += 1
            end
        else
            until left_it >= left_arr.length do
                return_arr.push(left_arr[left_it])
                left_it += 1
            end
        end
        return return_arr
    end
    #sorts an array of hashes where the hash table contains column names and values for a row.
    #sorts by @order column specified by where
    def mergeSort(arr, left, right)
        mid = left + ((right - left) / 2)
        if left >= right
            return arr[left..right]
        end
        left_arr = mergeSort(arr, left, mid)
        right_arr = mergeSort(arr, mid + 1, right)
        #puts "arrays to be merged = #{left_arr} with #{right_arr}"
        #puts "#{left} #{mid} #{right}"
        j = merge(left_arr, right_arr)
        return j
    end
    def checkColumnName(column_name)
        CSV.foreach(@table_name, 'r', headers:false) do |row|
            count = 0
            row.each do |header|
                if header == column_name
                    return true
                end
            end
            return false
        end
    end
    def order(order, column_name)
        @order = order
        @order_column = column_name
        self
    end

#Select functions
    def select(column_name)
        @operation = "select"
        if column_name.kind_of?(Array) == true
            column_name.each { |i| @columns.push(i) }
        else
            @columns.push(column_name)
        end
        self
    end
    def match(row)
        if row[@filter['column_name']] == @filter['criteria'] || (@filter.size() == 0)
            true
        else
            false
        end
    end
    def runSelect()
    
        @table.each do |row|
            #puts row
            #puts match(row)
            if match(row)
                row_mod = Hash.new
                @columns.each {|column_name| row_mod[column_name] = row[column_name]}
                @result.push(row_mod);
            end
        end
        if @order == "ASC" || @order == "DESC"
            arr_sorted = self.mergeSort(@result, 0, @result.length-1)
            return arr_sorted
        end
        @result
    end

#join functions
    def join(column_on_db_a, filename_db_b, column_on_db_b)
        @operation = "join"
        @join_table = CSV.read(filename_db_b, headers: true)
        @join_columns.push(column_on_db_a)
        @join_columns.push(column_on_db_b)
        CSV.foreach(filename_db_b, 'r', headers:true) do |row|
            if !@join_column_hash.has_key?(row[self.getJoinColumnB()])
                @join_column_hash[row[self.getJoinColumnB()]] = 1
            end
        end
        self
    end
    def joinTest(column_on_db_a, filename_db_b, column_on_db_b)
        @operation = "join"
        @join_columns.push(column_on_db_a)
        @join_columns.push(column_on_db_b)
        @join_table = CSV.read(filename_db_b, headers: true)
        @order = "ASC"
        @order_column = self.getJoinColumnB()
        CSV.foreach(filename_db_b, 'r', headers:true) do |row|
            if !@join_column_hash.has_key?(row[self.getJoinColumnB()])
                @join_column_hash[row[self.getJoinColumnB()]] = 1
            end
        end
        self
    end
    #if the column value in @table exists in the the specified column in @join_table return true
    def joinMatch(row)

        if @join_column_hash[row[getJoinColumnA()]] == 1
            return true
        end
        false
    end
    def runJoin()
        @table.each do |row|
            if joinMatch(row)
                row_mod = Hash.new
                @columns.each {|column_name| row_mod[column_name] = row[column_name]}
                @result.push(row_mod);
            end
        end
        if @order == "ASC" || @order == "DESC"
            arr_sorted = self.mergeSort(@result, 0, @result.length-1)
            puts arr_sorted
            return arr_sorted
        end
        @result
    end

#insert functions
    def insert(table_name)
        @operation = 'insert'
        @table_name = table_name
        @table = CSV.read(table_name, headers: true)
        self
    end
    def values(data)
        @data = data
        self
    end
    def runInsert()
        newRow = []
        @table.headers.each do |header|
            if @data[header] != nil
                newRow.push(@data[header])
            else
                newRow.push("")
            end
        end
        CSV.open(@table_name, 'a') do |obj|
                obj << newRow
        end
        newRow
    end

#update functions
    def update(table_name)
        @operation = 'update'
        @table_name = table_name
        @table = CSV.read(table_name, headers: true)
        self
    end
    def set()
        updated_rows = []
        @table.each do |row|
            if match(row)
                row.each do |key, val|
                    if @data.key?(key)
                        row[key] = @data[key]
                    end
                end
                updated_rows.push(row)
            end
        end
        CSV.open(@table_name, 'w') do |obj|
            obj << @table.headers
            @table.each do |row|
                obj << row
            end
        end
        updated_rows        
    end

#delete function
    def delete(table_name)
        @operation = 'delete'
        @table_name = table_name
        @table = CSV.read(table_name, headers: true)
        self
    end
    def runDelete
        puts @table.size
        index_to_delete = Array.new
        @table.each_with_index do | row, index|
            puts index
            puts match(row)
            if match(row)
                index_to_delete.push(index)
            end
        end
        count = 0
        puts index_to_delete
        index_to_delete.each do |i|
            @table.delete(i - count)
            count += 1
        end
        CSV.open(@table_name, 'w') do |obj|
            obj << @table.headers
            @table.each do |row|
                obj << row
            end
        end
    end
#run function
    def run()
        if @operation == 'select'
            return self.runSelect()
        elsif @operation == 'insert'
            self.runInsert
        elsif @operation == 'delete'
            self.runDelete()
        elsif @operation == 'update'
            self.set()
        elsif @operation == 'join'
            self.runJoin()
        end
    end
end
=begin
UPDATE
UPDATE SET name = DaVincÉ, year_start = 2021 WHERE name = Mike Wallace ORDER BY name ASC
=begin
SELECT
SELECT name, age FROM nba_player_data.csv WHERE name = Anakin ORDER BY name ASC;
SELECT name, year_start FROM nba_player_data.csv WHERE year_start = 1991 ORDER BY name ASC;

request = request.from("nba_player_data.csv")
request = request.where('year_start', '1980')
request = request.select(['name', 'position', 'weight', 'college'])
=end
=begin
#JOIN
SELECT name, height FROM nba_player_data.csv JOIN Seasons_Stats.csv ON name = Player;
request = request.from("nba_players.csv")
request = request.select(['Pid', 'Player', 'height', 'weight', 'college', ''])
request = request.join("Player", "Seasons_Stats.csv", "Player")
puts request.run
=end
=begin
#DELETE
DELETE FROM nba_player_data.csv WHERE name = Mike Wallace 
request = request.delete("nba_player_data.csv")
request = request.where('name', 'Obi Wan')
=end
=begin
#INSERT
INSERT INTO nba_player_data.csv (name, year_start, position) VALUES (Mike Wallace, 2020, C), (Willy Wonka, 2020, C);
request = request.insert('nba_player_data.csv')
request = request.values(data)
request.run
=end