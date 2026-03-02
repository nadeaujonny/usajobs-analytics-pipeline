"""
Federal Job Market Analytics Dashboard
Streamlit app for exploring federal job posting data
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# --- Page Config ---
st.set_page_config(
    page_title="Federal Job Market Analytics",
    page_icon="🏛️",
    layout="wide"
)

# --- Database Connection ---
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "jobs.db")

@st.cache_data(ttl=3600)
def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM job_postings", conn)
    conn.close()
    return df

df = load_data()

# --- Sidebar Filters ---
st.sidebar.title("🔍 Filters")

# Role category filter
all_roles = sorted(df["role_category"].dropna().unique())
selected_roles = st.sidebar.multiselect("Role Category", all_roles, default=all_roles)

# State filter
all_states = sorted(df["state"].dropna().unique())
selected_states = st.sidebar.multiselect("State", all_states)

# Department filter
all_depts = sorted(df["department_name"].dropna().unique())
selected_depts = st.sidebar.multiselect("Department", all_depts)

# Apply filters
filtered = df.copy()
if selected_roles:
    filtered = filtered[filtered["role_category"].isin(selected_roles)]
if selected_states:
    filtered = filtered[filtered["state"].isin(selected_states)]
if selected_depts:
    filtered = filtered[filtered["department_name"].isin(selected_depts)]

# --- Navigation ---
page = st.sidebar.radio("📊 Dashboard Page", [
    "Executive Overview",
    "Salary Analysis",
    "Geographic Demand",
    "Agency Analysis",
    "Data Explorer"
])

# ============================================================
# PAGE 1: EXECUTIVE OVERVIEW
# ============================================================
if page == "Executive Overview":
    st.title("🏛️ Federal Job Market Analytics")
    st.markdown("Real-time insights from USAJobs federal job postings")

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Postings", f"{len(filtered):,}")
    with col2:
        avg_salary = filtered["salary_mid"].dropna().mean()
        st.metric("Avg Salary (Mid)", f"${avg_salary:,.0f}" if pd.notna(avg_salary) else "N/A")
    with col3:
        unique_agencies = filtered["organization_name"].nunique()
        st.metric("Agencies Hiring", f"{unique_agencies:,}")
    with col4:
        unique_states = filtered["state"].dropna().nunique()
        st.metric("States with Postings", f"{unique_states}")

    st.divider()

    # Charts row 1
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Postings by Role Category")
        role_counts = filtered["role_category"].value_counts().reset_index()
        role_counts.columns = ["Role Category", "Count"]
        fig = px.bar(role_counts, x="Count", y="Role Category", orientation="h",
                     color="Count", color_continuous_scale="Blues")
        fig.update_layout(showlegend=False, height=400, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Postings by Department (Top 10)")
        dept_counts = filtered["department_name"].value_counts().head(10).reset_index()
        dept_counts.columns = ["Department", "Count"]
        fig = px.bar(dept_counts, x="Count", y="Department", orientation="h",
                     color="Count", color_continuous_scale="Greens")
        fig.update_layout(showlegend=False, height=400, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    # Charts row 2
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        st.subheader("Salary Distribution")
        salary_data = filtered["salary_mid"].dropna()
        if len(salary_data) > 0:
            fig = px.histogram(salary_data, nbins=30, labels={"value": "Salary (Midpoint)"},
                              color_discrete_sequence=["#636EFA"])
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No salary data available for current filters.")

    with col_right2:
        st.subheader("Top 10 States by Postings")
        state_counts = filtered["state"].value_counts().head(10).reset_index()
        state_counts.columns = ["State", "Count"]
        fig = px.bar(state_counts, x="State", y="Count", color="Count",
                     color_continuous_scale="Oranges")
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# PAGE 2: SALARY ANALYSIS
# ============================================================
elif page == "Salary Analysis":
    st.title("💰 Salary Analysis")

    salary_df = filtered.dropna(subset=["salary_mid"])

    if len(salary_df) == 0:
        st.warning("No salary data available for current filters.")
    else:
        # Summary stats
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Median Salary", f"${salary_df['salary_mid'].median():,.0f}")
        with col2:
            st.metric("Average Salary", f"${salary_df['salary_mid'].mean():,.0f}")
        with col3:
            st.metric("Min Salary", f"${salary_df['salary_mid'].min():,.0f}")
        with col4:
            st.metric("Max Salary", f"${salary_df['salary_mid'].max():,.0f}")

        st.divider()

        # Salary by role
        st.subheader("Salary by Role Category")
        role_salary = salary_df.groupby("role_category")["salary_mid"].agg(
            ["median", "mean", "count"]).reset_index()
        role_salary.columns = ["Role Category", "Median Salary", "Avg Salary", "Count"]
        role_salary = role_salary.sort_values("Median Salary", ascending=True)

        fig = px.bar(role_salary, x="Median Salary", y="Role Category", orientation="h",
                     hover_data=["Avg Salary", "Count"],
                     color="Median Salary", color_continuous_scale="Viridis")
        fig.update_layout(height=400, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

        # Salary by state (top 15)
        st.subheader("Median Salary by State (Top 15)")
        state_salary = salary_df.groupby("state")["salary_mid"].agg(
            ["median", "count"]).reset_index()
        state_salary.columns = ["State", "Median Salary", "Count"]
        state_salary = state_salary[state_salary["Count"] >= 3]  # Min 3 postings
        state_salary = state_salary.sort_values("Median Salary", ascending=False).head(15)

        fig = px.bar(state_salary, x="State", y="Median Salary",
                     hover_data=["Count"],
                     color="Median Salary", color_continuous_scale="RdYlGn")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

        # Salary by grade
        st.subheader("Salary by Job Grade")
        grade_salary = salary_df[salary_df["job_grade"].str.match(r"^\d+$", na=False)].copy()
        if len(grade_salary) > 0:
            grade_salary["job_grade_num"] = grade_salary["job_grade"].astype(int)
            grade_agg = grade_salary.groupby("job_grade_num")["salary_mid"].agg(
                ["median", "count"]).reset_index()
            grade_agg.columns = ["Grade", "Median Salary", "Count"]
            grade_agg = grade_agg.sort_values("Grade")

            fig = px.line(grade_agg, x="Grade", y="Median Salary",
                         markers=True, hover_data=["Count"])
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# PAGE 3: GEOGRAPHIC DEMAND
# ============================================================
elif page == "Geographic Demand":
    st.title("📍 Geographic Demand")

    # Map of postings
    geo_df = filtered.dropna(subset=["latitude", "longitude"])
    if len(geo_df) > 0:
        st.subheader("Job Postings Map")
        fig = px.scatter_geo(
            geo_df,
            lat="latitude",
            lon="longitude",
            hover_name="position_title",
            hover_data=["organization_name", "salary_mid", "state"],
            color="role_category",
            scope="usa",
            size_max=10,
        )
        fig.update_layout(height=500, geo=dict(bgcolor="rgba(0,0,0,0)"))
        st.plotly_chart(fig, use_container_width=True)

    # State breakdown
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Postings by State (Top 15)")
        state_counts = filtered["state"].value_counts().head(15).reset_index()
        state_counts.columns = ["State", "Count"]
        fig = px.bar(state_counts, x="State", y="Count", color="Count",
                     color_continuous_scale="Blues")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Postings by City (Top 15)")
        city_counts = filtered["city"].value_counts().head(15).reset_index()
        city_counts.columns = ["City", "Count"]
        fig = px.bar(city_counts, x="City", y="Count", color="Count",
                     color_continuous_scale="Purples")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Remote/multiple locations analysis
    st.subheader("Location Type Breakdown")
    location_type = filtered["location_name"].apply(
        lambda x: "Multiple Locations" if "Multiple" in str(x)
        else ("Negotiable" if "Negotiable" in str(x) or "Anywhere" in str(x)
        else "Single Location"))
    loc_counts = location_type.value_counts().reset_index()
    loc_counts.columns = ["Location Type", "Count"]
    fig = px.pie(loc_counts, values="Count", names="Location Type",
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# PAGE 4: AGENCY ANALYSIS
# ============================================================
elif page == "Agency Analysis":
    st.title("🏢 Agency & Department Analysis")

    # Top agencies
    st.subheader("Top 15 Hiring Organizations")
    org_counts = filtered["organization_name"].value_counts().head(15).reset_index()
    org_counts.columns = ["Organization", "Count"]
    fig = px.bar(org_counts, x="Count", y="Organization", orientation="h",
                 color="Count", color_continuous_scale="Reds")
    fig.update_layout(height=500, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

    # Department breakdown
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Postings by Department")
        dept_counts = filtered["department_name"].value_counts().reset_index()
        dept_counts.columns = ["Department", "Count"]
        fig = px.pie(dept_counts.head(10), values="Count", names="Department",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Avg Salary by Department (Top 10)")
        dept_salary = filtered.dropna(subset=["salary_mid"]).groupby(
            "department_name")["salary_mid"].agg(["mean", "count"]).reset_index()
        dept_salary.columns = ["Department", "Avg Salary", "Count"]
        dept_salary = dept_salary[dept_salary["Count"] >= 3]
        dept_salary = dept_salary.sort_values("Avg Salary", ascending=False).head(10)

        fig = px.bar(dept_salary, x="Avg Salary", y="Department", orientation="h",
                     hover_data=["Count"],
                     color="Avg Salary", color_continuous_scale="Viridis")
        fig.update_layout(height=400, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    # Role distribution by department
    st.subheader("Role Categories by Top Departments")
    top_depts = filtered["department_name"].value_counts().head(8).index
    dept_role = filtered[filtered["department_name"].isin(top_depts)]
    cross = pd.crosstab(dept_role["department_name"], dept_role["role_category"])
    fig = px.imshow(cross, color_continuous_scale="Blues", aspect="auto")
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# PAGE 5: DATA EXPLORER
# ============================================================
elif page == "Data Explorer":
    st.title("🔎 Data Explorer")
    st.markdown("Browse and search the raw data")

    # Search
    search_term = st.text_input("Search position titles", "")
    if search_term:
        display_df = filtered[filtered["position_title"].str.contains(search_term, case=False, na=False)]
    else:
        display_df = filtered

    st.markdown(f"**Showing {len(display_df):,} postings**")

    # Column selection
    display_cols = ["position_title", "organization_name", "department_name",
                    "salary_mid", "state", "role_category", "open_date"]
    available_cols = [c for c in display_cols if c in display_df.columns]

    st.dataframe(
        display_df[available_cols].sort_values("open_date", ascending=False),
        use_container_width=True,
        height=600
    )

    # Download button
    csv = filtered.to_csv(index=False)
    st.download_button("📥 Download Filtered Data (CSV)", csv,
                       "federal_jobs_filtered.csv", "text/csv")


# --- Footer ---
st.sidebar.divider()
st.sidebar.caption("Data source: USAJobs.gov API")
st.sidebar.caption(f"Total records in database: {len(df):,}")