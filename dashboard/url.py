from django.urls import path
from . import views 

urlpatterns = [
    # path('',views.home,name='home'),
    path('',views.team,name='team'),
    path('marketing',views.marketing,name='marketing'),
    path('rep/<str:rep>//<str:asm>/',views.kokun,name='rep'),
    path('area/<str:asm>//<str:reg>/',views.area,name='area'),
    path('region/<str:reg>/',views.region,name='region'),
    path('distributors/<str:dist>/',views.distributors,name='distributors'),
    path('zen',views.potus),
]
