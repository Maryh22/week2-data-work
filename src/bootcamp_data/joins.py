import pandas as pd

import pandas as pd

def safe_left_join(orders: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
   
    joined = pd.merge(orders,users,on="user_id", how="left")
    return joined
"""
SELECT *
FROM df1
LEFT OUTER JOIN df2
  ON df1.key = df2.key;
  هنا سوينا left joion
  المثود تستقل اثنين داتا فريم الي تسوي لهم جوين

  استدعينا مثود ميرج حطينا فيها الدتار الفريم الفت الي هي order الرايت الي هي user 
  والبوسط نحدد key 
  how =left---->تحدد ان الجوين بتكون لفت 

"""