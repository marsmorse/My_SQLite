require_relative 'my_sqlite_parser.rb'
require_relative 'my_sqlite_request.rb'
require_relative 'validator.rb'
select_tests = [
    { 'test_input': "SELECT name, year_start;", test_desc:"Testing SELECT without a FROM"},
    #{ 'test_input': "SELECT name, year_start FROM table;", test_desc:"Testing SELECT with FROM where table input is wrong"},
    { 'test_input': "SELECT FROM nba_player_data;", test_desc:"Testing SELECT with FROM where no Columns are input"},
    { 'test_input': "SELECT sun, moon FROM nba_player_data;", test_desc:"Testing SELECT with FROM where Columns don't exist in the input table"},
    { 'test_input': "SELECT * FROM nba_player_data.csv;", test_desc:"Testing SELECT with FROM with * as select columns"},
    { 'test_input': "SELECT name, year_start FROM;", test_desc:"Testing SELECT with FROM where no table is input"},
    { 'test_input': "SELECT name, year_startFROM nba_player_data.csv;", test_desc:"Testing SELECT without a space before the FROM"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv WHERE name = ;", test_desc:"Testing SELECT with WHERE column name DNE"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv WHERE = Death Vader;", test_desc:"Testing SELECT with WHERE when column criteria DNE"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv WHERE sun = Death Vader;", test_desc:"Testing SELECT with WHERE when column name doesn't exist in table"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv WHERE;", test_desc:"Testing SELECT with WHERE when entire inequality DNE"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv ORDER BY name ASC;", test_desc:"Testing SELECT with ORDER BY name ASC"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv ORDER BY name DESC;", test_desc:"Testing SELECT with ORDER BY name DESC"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv ORDER BY;", test_desc:"Testing SELECT with ORDER BY when clause is empty"},
    #{ 'test_input': "SELECT name, year_start FROM nba_player_data.csv ORDER BY cheese DESC;", test_desc:"Testing SELECT with ORDER BY where column to order on is invalid"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv ORDER BY name LIT;", test_desc:"Testing SELECT with ORDER BY when order is a garbage value"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv ORDER BY name;", test_desc:"Testing SELECT with ORDER BY when no sort order specified"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data JOIN nba_players ON nba_player_data.name = nba_players.Player;", test_desc:"Testing JOIN with correct"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv;", test_desc:"Testing SELECT with FROM correct"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv ORDER BY name ASC;", test_desc:"Testing SELECT with ORDER BY name ASC"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv ORDER BY name DESC;", test_desc:"Testing SELECT with ORDER BY name DESC"},
    { 'test_input': "SELECT name, year_start FROM nba_player_data.csv WHERE name=Death Vader;", test_desc:"Testing SELECT with WHERE without spaces between the equals"}
]

insert_tests = [
    { 'test_input': "INSERT INTO nba_player_data.csv (name, year) VALUES (Hannah Montana, 2006);", test_desc:"Testing INSERT whith faulty column name"},
    { 'test_input': "INSERT INTO nba_player_data.csv (name, year) VALUES ;", test_desc:"Testing INSERT whith missing VALUES content"},
    { 'test_input': "INSERT INTO nba_player_data.csv VALUES (Hannah Montana, 2006);", test_desc:"Testing INSERT when no columns are specified"},
    { 'test_input': "INSERT INTO nba_player_data.csv (name, year) (Hannah Montana, 2006);", test_desc:"Testing INSERT without VALUES keyword"},
    { 'test_input': "INSERT nba_player_data.csv (name, year) VALUES (Hannah Montana, 2006);", test_desc:"Testing INSERT when INTO keyword is missing"},
    { 'test_input': "INSERT INTO nba_player_data.csv (name, year_start) VALUES (Mike Ditka, 1982);", test_desc:"Testing INSERT with correct VALUES"},
    { 'test_input': "INSERT INTO nba_player_data.csv (name,year_start,year_end,position,height,weight,birth_date,college) VALUES (Jim Beans,1995,2010,165,240, 'June 24, 1968', Duke University);", test_desc:"Testing INSERT with full row and no space between items"}
]

update_tests = [
    { 'test_input': "UPDATE nba_player_data.csv SET WHERE;", test_desc:"Testing UPDATE with missing set values"},
    { 'test_input': "UPDATE nba_player_data.csv SET name = John Smith WHERE;", test_desc:"Testing UPDATE with missing where values"},
    { 'test_input': "UPDATE nba_player_data.csv SET name = Jake Warner, height = 69 WHERE year_start = 1991;", test_desc:"Testing UPDATE with multile set values"},
    { 'test_input': "UPDATE nba_player_data.csv SET name = John Mits WHERE year_start = 1991;", test_desc:"Testing UPDATE with correct"}
]

delete_tests = [
    #{ 'test_input': "DELETE FROM nba_player_data.csv WHERE name = Jim Beans;", test_desc:"Testing DELETE with correct"},
    { 'test_input': "DELETE FROM nba_player_data.csv;", test_desc:"Testing DELETE with no where statement"},
    { 'test_input': "DELETE FROM WHERE name = Shareef Abdur-Rahim;", test_desc:"Testing DELETE with missing table name"},
    #{ 'test_input': "DELETE FROM nba_player_data.csv WHERE;", test_desc:"Testing DELETE with missing where values"},
]
def runSelect(sql_parser)
    request = MySqliteRequest.new
    keyword_flags = sql_parser.getFlags()
    validator = Validator.new
    if validator.validTableName(sql_parser.getTableName)
        request = request.from(sql_parser.getTableName)
        puts "Valid Table Name"
        s_data = sql_parser.getSelectData        
        if !validator.validSelectValues(s_data)#!###
            return 0
        end
        puts "Valid Select Values"
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
def runInput(requestString)
    sql_parser = SQLParser.new(requestString)
    sql_parser.Parse()
    sql_parser.sanitizeTableName()
    if sql_parser.getError() > 0
        puts sql_parser.getErrorMsg()
        return 
    end
    #validate input
    if ARGV[0] == 'select'
        result = runSelect(sql_parser)
    elsif ARGV[0] == 'insert'
        result = runInsert(sql_parser)

    elsif ARGV[0] == 'update'
        result = runUpdate(sql_parser)
    elsif ARGV[0] == 'delete'
        result = runDelete(sql_parser)
    end
    return result
end
def runTest(test_desc, test_input)
        puts "--------------------"
        puts "#{test_desc}"
        puts "#{test_input}"
        puts "--------------------"
        result = runInput(test_input)
        if result == nil
            puts "Error with request"
            return 1
        else
            puts result
            return 1
        end
end

def testOp(test_hash, test_name)
    puts "--------------------"
    puts "\n"
    puts "TESTING #{test_name}"
    puts "\n"
    puts "--------------------"
    score = 0
    test_hash[test_name].each do |t|
        puts "TEST #{score}"
        score += runTest(t[:test_desc], t[:test_input].chop)
    end
end
def run(test_hash)
    if ARGV[0] == 'select'
        testOp(test_hash, 'select')
    elsif ARGV[0] == 'insert'
        testOp(test_hash, 'insert')
    elsif ARGV[0] == 'update'
        testOp(test_hash, 'update')
    elsif ARGV[0] == 'delete'
        testOp(test_hash, 'delete')
    end
end

test_hash = Hash.new
test_hash["select"] = select_tests
test_hash["insert"] = insert_tests
test_hash["update"] = update_tests
test_hash["delete"] = delete_tests
run(test_hash)