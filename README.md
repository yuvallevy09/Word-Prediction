# WordPrediction

**Contributors:**

    Yuval Levy (209792654), Hagar Samimi-Golan (206396079)

**How to run the project:**

    Upload JAR files of step 1 and step 2 to S3, define the number of instances in “App” and run the project.

**Map-Reduce Logic:**

    Step 1:
    Purpose: Collects and processes n-gram counts to prepare for probability calculations.
    Key Components:
    Data Processing Flow:
    Takes input from three sources: unigrams (1-gram), bigrams (2-gram), and trigrams (3-gram)
    Filters out stop words (common Hebrew words that would skew predictions)
    Calculates intermediate counts needed for probability calculations
    CompositeKey Class:
    Used to structure the map outputs with fields:
    type: "N" (for N-gram counts) or "C" (for conditional counts)
    w1, w2, w3: The words in the n-gram
    Has custom comparison logic to ensure proper grouping and sorting
    The key comparison determines the order in which each reducer processes its records:
    For N-type records (ordered to calculate N1, N2, N3):
    Primary sort by w3 (matches partitioning)
    Then by w2, with "*" coming first
    Finally by w1, with "*" coming first
    For C-type records (ordered to calculate C1, C2):
    Primary sort by w2 (matches partitioning)
    Then by w1, with "*" coming first
    
    Ordering ensures counts (N1, N2, N3, C1, C2) are processed in the correct order to calculate probabilities
    The "*" ordering ensures denominator values (total counts) are processed before the specific n-gram counts that need them
    
    
    Mapper Classes:
    UniMapper: Processes single words
    Emits two types of records:
    N-type: (*, *, word) → count
    C-type: (*, word, *) → count
    BiMapper: Processes word pairs
    Emits records like:
    N-type: (*, w1, w2) → count
    C-type: (w1, w2, *) → count
    TriMapper: Processes three-word sequences
    Emits N-type records: (w1, w2, w3) → count
    Note: The C0 count (total number of word instances in the corpus) is tracked using a Hadoop Counter in the UniMapper class. The C0 value is accumulated during the processing of unigrams (single words). At the end of Step1, after the job completes, the code retrieves the counter value and writes it to a separate file. The value is shared across reducers in Step2 via a file, avoiding the need to recalculate it. The counter automatically handles the aggregation across all mappers, so we don't need to write special logic for combining partial counts.
    Partitioner:
    
    
    This partitioner determines which reducer gets each key-value pair:
    For N-type records: partitioned by w3 (the target word)
    For C-type records: partitioned by w2
    This ensures all records needed to calculate probabilities for a specific word end up in the same reducer
    
    NGramReducer:
    Aggregates counts for each type of n-gram
    Tracks denominators (C0, C1, C2) needed for probability calculations
    Outputs records with all necessary counts for probability calculation
    
    Step 2:
    Purpose: Calculates final probabilities using the counts from Step 1
    Key Components:
    TrigramKey Class:
    Simpler key structure focusing on the three words
    Used to group related counts together
    Step2Mapper:
    Processes output from Step 1
    Separates C-type and N-type records for probability calculations
    ProbabilityReducer:
    Implements the actual probability calculation using the formula
    The final output is sorted:
    First by word pairs (w1,w2) in ascending order
    Then by probability for each w3 in descending order





**10 Pairs with their top 5 most probable words:**
   
     בתי מדרש:
    למורים: 0.779
    וישיבות: 0.137
    הרבנים: 0.135
    לתורה: 0.134
    החכמה: 0.110
    
    היום יום:
    ראשון: 0.037
    רביעי: 0.016
    שלישי: 0.015
    טוב: 0.014
    חמישי: 0.013
    
    חבר ועד:
    הלשון: 0.681
    הקהילה: 0.282
    הרבנים: 0.236
    הצירים: 0.161
    הפועל: 0.057
    
    הגאון הרב:
    יוסף: 0.052
    יצחק: 0.031
    אברהם: 0.029
    שלמה: 0.029
    משה: 0.027
    
    תרומה חשובה:
    ביותר: 0.346
    מאוד: 0.132
    לחקר: 0.039
    לתולדות: 0.036
    להבנת: 0.035
    
    עמוד הענן:
    יומם: 0.094
    ועמוד: 0.050
    מפניהם: 0.041
    ועמד: 0.027
    עומד: 0.015
    
    ריח רע:
    נודף: 0.018
    שיש: 0.018
    שאין: 0.014
    עולה: 0.012
    מפיו: 0.010
    
    קשה גם:
    להניח: 0.026
    להבין: 0.021
    לדעת: 0.015
    לקבל: 0.014
    לקבוע: 0.009
    
    נבנה בית:
    המקדש: 0.280
    הכנסת: 0.195
    כנסת: 0.083
    המדרש: 0.017
    הבחירה: 0.011
    
    לבקר בבית:
    הספר: 0.126
    הקברות: 0.089
    החולים: 0.063
    הכנסת: 0.048
    ספר: 0.042

**Reports:**

**With local aggregation**

    Step1:
    Key-value pairs sent: 213,616,669
    Size of data sent: 800,691,937 bytes (~800 MB)

    Step2:
    Key-value pairs sent: 1,721,484
    Size of data sent: 32,984,523 bytes (~33 MB)
    (Step2 is without combiner)

    Number of instances: 9
    Total time taken: 15 minutes 

    Number of instances: 5
    Total time taken: 35 minutes 
****
**Without local aggregation**
    
    Step1:
    Key-value pairs sent: 348608126
    Size of data sent: 1242826247 (~1184 MB)
    Total time taken: 47 minutes

****
**Small input text file:(20 3-gram)**

    Number of instances: 1
    Total time taken: 1 minute 30 seconds 

    Number of instances: 2
    Total time taken: 1 minute 


