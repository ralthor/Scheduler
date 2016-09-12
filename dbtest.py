import sys
import db.reader
import db.definitions


def tests():
    reader = db.reader.Reader('mydb')
    rows = reader.read_test('2')
    row = rows.fetchone()
    test = db.definitions.Test(row)
    print(test.test_name)
    print(len(test.resource_array))
    for r in test.resource_array:
        print(r)  # r[0] power, r[1] price, r[2] number


def main(args):
    if len(args) < 1:
        print('Required command line arguments are not specified: make_test or make_super_test or planner?')
        exit()
    elif args[0] == 'make_test':
        if args[1] == '-h':
            print("params are: "
                  "test_name "
                  "node_name "
                  "test_numbers_from test_numbers_to "
                  "dbfilename filenamepart start_number number_of_test_sets policy")
        else:
            test_name = args[1]
            node_name = args[2]
            n_from = int(args[3])
            n_to = int(args[4])
            dbfilename = args[5]
            file_name_part = args[6]
            start_number = int(args[7])
            number_of_test_sets = int(args[8])
            policy = args[9]
            for i in range(n_from, n_to + 1):
                print("ssh {} './mystarter.sh {} {} {} {} {} {} >./results/test{:0>3d}.txt &"
                      "'".format(node_name,
                                 test_name, dbfilename, file_name_part, start_number, number_of_test_sets, policy,
                                 i))
    elif args[0] == 'make_super_test':
        if args[1] == '-h':
            print("params are: test_name run_on_each_node "
                  "dbfilename filenamepart start_number number_of_test_sets policy"
                  " node_name1 node_name2 node_name3 ...")
        else:
            test_name = args[1]
            jump = int(args[2])
            dbfilename = args[3]
            file_name_part = args[4]
            start_number = int(args[5])
            number_of_test_sets = int(args[6])
            policy = args[7]
            start = 1
            for node_name in args[8:]:
                print("python dbtest.py make_test"
                      " {} {} {} {} {} {} {} {} {}"
                      "".format(test_name, node_name, start, start + jump - 1,
                                dbfilename, file_name_part, start_number, number_of_test_sets, policy))
                start += jump
    elif args[0] == 'planner':
        if args[1] == '-h':
            print("params are: test_name dbname test_numbers_from test_numbers_to")
        else:
            test_name = args[1]
            db_name = args[2]
            n_from = int(args[3])
            n_to = int(args[4])
            #  print("mkdir $1")
            for i in range(n_from, n_to + 1):
                print("python planner.py {0} {1} ../plans/{0}/save{2:0>3}.p "
                      "> outputs/{0}_{2:0>3}.txt &".format(test_name, db_name, i))


if __name__ == "__main__":
    main(sys.argv[1:])
