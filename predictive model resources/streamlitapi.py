import pickle 
import streamlit as st
import os
import pandas as pd
import numpy as np  # Import numpy for clipping

# Load the model
model_path = os.path.join(os.path.dirname(__file__), 'Student_Performance_Prediction_Model.pkl') 
model = pickle.load(open(model_path, 'rb'))

def main():
    st.title('Student Performance Prediction')

    # Input variables with placeholders
    Age = st.text_input('Age of student (years)', placeholder="e.g., 18")
    Library = st.text_input('Hours in the library per week', placeholder="e.g., 5")
    Class = st.text_input('Hours of class attendance per week', placeholder="e.g., 20")
    Extracurricular = st.text_input('Hours of extra-curricular activities per week', placeholder="e.g., 10")
    
    # Error flag to track if all inputs are valid
    error = False

    # Only validate inputs after the user clicks 'Predict'
    if st.button('Predict'):
        # Validation for each input (ensure they are floats)
        if Age:
            try:
                Age = float(Age)
            except ValueError:
                st.error("Please enter a valid number for Age.")
                error = True
        else:
            st.error("Age is required.")
            error = True

        if Library:
            try:
                Library = float(Library)
            except ValueError:
                st.error("Please enter a valid number for Library weekly hours.")
                error = True
        else:
            st.error("Library weekly hours is required.")
            error = True

        if Class:
            try:
                Class = float(Class)
            except ValueError:
                st.error("Please enter a valid number for Class weekly attendance hours.")
                error = True
        else:
            st.error("Class weekly attendance hours is required.")
            error = True

        if Extracurricular:
            try:
                Extracurricular = float(Extracurricular)
            except ValueError:
                st.error("Please enter a valid number for Extra-curricular weekly hours.")
                error = True
        else:
            st.error("Extra-curricular weekly hours is required.")
            error = True

        # Only proceed with prediction if there are no errors
        if not error:
            # Create a DataFrame with the correct column names
            input_data = pd.DataFrame([[Age, Library, Class, Extracurricular]], 
                                      columns=['Age', 'Library_weekly_hours', 'Class_weekly_attendance_hours', 'Extra-curricular_weekly_hours'])
            
            # Perform the prediction
            makeprediction = model.predict(input_data)
            score = makeprediction[0]
            
            # Step to apply clipping between 0 and 100
            score_clipped = np.clip(score, 0, 100)

            # Display the result
            st.success(f'This student is expected to score {score_clipped:.0f} in their upcoming exam')

if __name__ == '__main__':
    main()
