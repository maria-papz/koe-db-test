from django.urls import path, re_path
from . import api_views, workflow_views
from .views import(
    CustomProviderAuthView,
    CustomTokenObtainPairView,
    CustomTokenRefreshView,
    CustomTokenVerifyView,
    LogoutView,
)


urlpatterns = [
    # path('login/', views.login_view, name='login'),
    # path('', views.homepage, name='homepage'),
    # path('view_table/<str:table_name>/', views.view_table, name='view_table'),
    # path('all_tables/', views.view_all_tables, name='view_all_tables'),
    # path('logout/', views.logout_view, name='logout'),
    # path('manage_permission_requests/', views.manage_permission_requests, name='manage_permission_requests'),
    # path('hello-world/', views.hello_world, name='hello_world'),
    re_path(
        r'^api/o/(?P<provider>\S+)/$',
        CustomProviderAuthView.as_view(),
        name='provider-auth'
    ),

    path('api/jwt/create/', CustomTokenObtainPairView.as_view()),
    path('api/jwt/refresh/', CustomTokenRefreshView.as_view()),
    path('api/jwt/verify/', CustomTokenVerifyView.as_view()),
    path('api/logout/', LogoutView.as_view()),

    # Indicator Filtering
    path('api/boolean-filter/', api_views.boolean_filter, name='choose indicator'),
    path('api/available-fields/', api_views.get_available_fields, name='get available fields'),


    # API Endpoints
    # path('api/delete-table/', api_views.delete_table, name='delete table'),
    path('api/tables/', api_views.add_view_table, name='add table/view all tables'),
    path('api/tables/<str:id>/', api_views.tables, name='view/delete table'),
    path('api/indicators/<str:id>/', api_views.indicators, name='view indicators'),
    path('api/indicators/', api_views.add_view_indicators, name='add indicators/view all indicators'),
    path('api/tables/<str:id>/indicators', api_views.add_indicators_to_table, name='add indicators to table'),
    path('api/tables/<str:table_id>/indicators/<str:indicator_id>/', api_views.delete_table_indicator, name='delete indicator from table'),
    path('api/categories/', api_views.add_view_category, name= 'add category/view all categories'),
    path('api/countries/', api_views.add_view_country, name= 'add country/view all countries'),
    path('api/regions/', api_views.add_view_region, name= 'add region/view all regions'),
    path('api/indicator/codes/', api_views.codes, name='retrieve all indicator codes'),
    path('api/countries/codes/', api_views.country_codes, name='retrieve all country codes'),
    path('api/units/', api_views.add_view_unit, name= 'add unit/view all units'),
    path('api/custom_indicators/<str:indicator_id>/', api_views.create_custom_indicator, name= 'add/view custom indicators'),
    path('api/data/<str:indicator_id>/', api_views.data, name= 'add data to indicator'),
    path('api/indicator/<str:indicator_id>/history/', api_views.indicator_history, name= 'view indicator history'),

    # Add these new endpoints
    path('api/indicators/<str:indicator_id>/permissions/', api_views.manage_indicator_permissions, name='indicator_permissions'),
    path('api/users/', api_views.get_users, name='get_users'),

    # Add the new user activity endpoint
    path('api/users/<str:user_id>/activity/', api_views.user_activity, name='user_activity'),

    # User favorites and follows
    path('api/indicators/<str:indicator_id>/favourites/', api_views.favourite_indicator, name='favourite_indicator'),
    path('api/tables/<str:table_id>/favourites/', api_views.favourite_table, name='favourite_table'),
    path('api/users/<str:user_id>/follow/', api_views.follow_user, name='follow_user'),
    path('api/users/me/following/', api_views.get_user_following, name='get_user_following'),
    path('api/users/me/following/activity/', api_views.followed_user_activity, name='followed user activity'),
    path('api/users/me/favourites/indicators/activity', api_views.favourite_indicator_activity, name='get_user_favourites'),

    # Add this new endpoint
    path('api/indicators/<str:indicator_id>/restore-data', api_views.restore_indicator_data, name='restore_indicator_data'),

    # Workflow management endpoints
    path('api/workflows/', workflow_views.workflows, name='workflows'),
    path('api/workflows/<int:id>/', workflow_views.workflow_detail, name='workflow_detail'),
    path('api/workflows/<int:id>/run/', workflow_views.workflow_run, name='workflow_run'),
    path('api/workflows/<int:workflow_id>/run_history/', workflow_views.workflow_run_history, name='workflow_run_history'),
    path('api/workflows/<int:id>/toggle/', workflow_views.workflow_toggle, name='workflow_toggle'),
    path('api/workflows/<int:id>/history/', workflow_views.workflow_history, name='workflow_history'),
    path('api/workflows/indicator/<str:indicator_id>/', workflow_views.workflows_by_indicator, name='workflows_by_indicator'),
    path('api/workflows/latest/', workflow_views.latest_workflow_run, name='latest_workflow_run'),
    path('api/cystat-workflow-config/', workflow_views.cystat_workflow_config, name='cystat_workflow_config'),
    path('api/cystat-indicator-mapping/', workflow_views.cystat_indicator_mapping, name='cystat_indicator_mapping'),
    path('api/fetch-cystat-structure/', workflow_views.fetch_cystat_structure, name='fetch_cystat_structure'),
    # path('api/cystat-workflow-details/<int:id>/', workflow_views.cystat_workflow_details, name='cystat_workflow_details'),

    # ECB workflow endpoints
    path('api/ecb-workflow-config/', workflow_views.ecb_workflow_config, name='ecb_workflow_config'),
    path('api/fetch-ecb-structure/', workflow_views.fetch_ecb_structure, name='fetch_ecb_structure'),
    # path('api/ecb-workflow-details/<int:id>/', workflow_views.ecb_workflow_details, name='ecb_workflow_details'),

    # Eurostat workflow endpoints
    path('api/eurostat-workflow-config/', workflow_views.eurostat_workflow_config, name='eurostat_workflow_config'),
    path('api/eurostat-indicator-mapping/', workflow_views.eurostat_indicator_mapping, name='eurostat_indicator_mapping'),
    path('api/fetch-eurostat-structure/', workflow_views.fetch_eurostat_structure, name='fetch_eurostat_structure'),
]
