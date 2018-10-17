def get_hamming_score(list1, list2):
    hamming_score = 0
    mismatch_position = ""
    for index in range(len(list1)):
        if list1[index] != list2[index]:
            hamming_score += 1
            mismatch_position = str(index) + ","

    mismatch_position = mismatch_position.strip(',')
    print (hamming_score, mismatch_position)
    return hamming_score, mismatch_position


get_hamming_score([1,2,3], [1,2,4])




