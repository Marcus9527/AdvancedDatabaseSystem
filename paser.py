import re


def parser(input_file):
    infile = open(input_file, 'r')
    lines = infile.readlines()
    line_num = 0
    for line in lines:
        line_num += 1
        try:
            line = line.strip('\n')
            operation = re.split('\(|\)', line)
            operation_name = operation[0]

            if len(operation) < 2:
                errmsg = "missing parameters for ["+operation_name+"] in line "+str(line_num)
                raise ValueError(errmsg)
            else:
                operation_arg = re.split(',', operation[1])

            if operation_name == "begin":
                pass;
            elif operation_name == "beginRO":
                pass
            elif operation_name == "R":
                pass
            elif operation_name == "W":
                pass
            elif operation_name == "dump":
                pass
            elif operation_name == "end":
                pass
            elif operation_name == "fail":
                pass
            elif operation_name == "recover":
                pass
            else:
                errmsg = "can not recognize operation name: ["+operation_name+"] in line "+str(line_num)
                raise ValueError(errmsg)

        except ValueError as err:
            print(err.args)


if __name__ == "__main__":
    parser("input")