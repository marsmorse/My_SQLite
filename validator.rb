class Validator
    def initialize

    end
    #
    # SELECT validation functions
    #
    def validTableName(table_name)
        if File.file?(table_name)
            return true
        end
        return false
    end
    def validSelectValues(s_vals)
        return true
    end
    def validWhere(col_name, col_val)
        if col_name != nil && col_val != nil
            return true
        end
        return false
    end
    def validOrder(col_name, order)
        if col_name != nil && order != nil
            return true
        end
        return false
    end

    #
    # INSERT validation functions
    #
    def validValues

    end
end
