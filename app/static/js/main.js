document.addEventListener('DOMContentLoaded', function() {
    const killButtons = document.querySelectorAll('.kill-profile');

    killButtons.forEach(button => {
        button.addEventListener('click', function(event) {
            const profileId = this.dataset.profileId;

            fetch(`/kill_profile/${profileId}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    alert('Profile killed successfully!');
                    location.reload();
                } else {
                    alert('Error killing profile: ' + data.message);
                }
            })
            .catch(error => {
                console.error('Error:', error);
                alert('An error occurred while trying to kill the profile.');
            });
        });
    });
});