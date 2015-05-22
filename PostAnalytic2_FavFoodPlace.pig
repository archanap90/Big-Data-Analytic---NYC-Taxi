A = Load 'FavFoodPlace/part-r-00000' as (Time, Destination, Count);
Lunch= FILTER A by (Time == '12-13') OR (Time == '13-14');
STORE Lunch into 'FavLunchPlace';
Breakfast = FILTER A by (Time == '21-22') OR (Time =='22-23');
STORE Lunch into 'FavBreakfastPlace';
Dinner = FILTER A by (Time == '08-9') OR (Time== '09-10');
STORE Lunch into 'FavDinnerPlace';
LateNight= FILTER A by (Time == '00-1') OR (Time== '01-2');
STORE Lunch into 'FavNightOutPlace';


